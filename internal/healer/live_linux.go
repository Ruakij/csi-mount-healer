//go:build linux

package healer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
)

// swapMounts replaces what is mounted at each path in the mount namespace of pid
// with a clone of the new mount on the node, and returns the inode of that
// namespace and the ids of the replaced mounts. Other mounts right below a
// replaced one are mounted again on the new mount from their sources, which
// map each container path the runtime bound to the host path it bound from,
// in the mount namespace hostNS.
func swapMounts(pid int, mounts []liveMount, sources map[string]liveMount, hostNS string) (uint64, map[uint64]bool, error) {
	ns, err := nsInode(fmt.Sprintf("/proc/%d", pid))
	if err != nil {
		return 0, nil, err
	}
	// Mount points relative to the root of pid, which is the root of its namespace.
	infos, err := readMountinfo(fmt.Sprintf("/proc/%d", pid))
	if err != nil {
		return 0, nil, err
	}
	// A swap detaches the mounts below its path, so one of those that is swapped
	// as well goes after it.
	mounts = slices.Clone(mounts)
	slices.SortFunc(mounts, func(a, b liveMount) int { return strings.Compare(a.path, b.path) })
	swapped := map[string]bool{}
	for _, m := range mounts {
		swapped[m.path] = true
	}
	dead := map[uint64]bool{}
	ids := make([]uint64, len(mounts))
	// A lookup below a dead FUSE mount fails revalidation, and the kernel then
	// detaches the mounts on that dentry, so children are cloned from their
	// sources, never through their path in the container.
	carry := make([][]child, len(mounts))
	defer func() {
		for _, cs := range carry {
			for _, c := range cs {
				closeFD(c.clone)
				closeFD(c.at)
			}
		}
	}()
	for i, m := range mounts {
		for _, mi := range infos {
			if mi.point == m.path {
				// The last one is on top.
				ids[i] = mi.id
			}
		}
		if ids[i] == 0 {
			return 0, nil, fmt.Errorf("nothing is mounted at %s", m.path)
		}
		dead[ids[i]] = true
		for _, point := range childPoints(infos, ids[i], m.path, swapped) {
			src, ok := sources[point]
			if !ok {
				return 0, nil, fmt.Errorf("%s is mounted below %s, but not by the container runtime, so its source is unknown", point, m.path)
			}
			carry[i] = append(carry[i], child{point: point, src: src, clone: -1, at: -1})
		}
	}

	var clones []int
	defer func() {
		for _, fd := range clones {
			unix.Close(fd)
		}
	}()
	for _, m := range mounts {
		fd, err := cloneMount(m)
		if err != nil {
			return 0, nil, fmt.Errorf("cloning the new mount for %s: %w", m.path, err)
		}
		clones = append(clones, fd)
	}
	// Every child is cloned and has its mount point in the new mount before
	// anything changes, so a swap that cannot carry one leaves all in place.
	if slices.ContainsFunc(carry, func(cs []child) bool { return len(cs) > 0 }) {
		// Host paths of the runtime may lie outside what the healer mounts.
		if err := inMountNS(hostNS, 0, func() error {
			for _, cs := range carry {
				for j := range cs {
					fd, err := cloneSource(cs[j].src)
					if err != nil {
						return fmt.Errorf("cloning %s for %s: %w", cs[j].src.target, cs[j].point, err)
					}
					cs[j].clone = fd
				}
			}
			return nil
		}); err != nil {
			return 0, nil, err
		}
	}
	for i, m := range mounts {
		for j := range carry[i] {
			if err := openMountPoint(clones[i], m.path, &carry[i][j]); err != nil {
				return 0, nil, err
			}
		}
	}

	return ns, dead, inMountNS(fmt.Sprintf("/proc/%d/ns/mnt", pid), ns, func() error {
		now, err := readMountinfo("/proc/thread-self")
		if err != nil {
			return err
		}
		for i, m := range mounts {
			points := make([]string, len(carry[i]))
			for j, c := range carry[i] {
				points[j] = c.point
			}
			if !slices.Equal(childPoints(now, ids[i], m.path, swapped), points) {
				return fmt.Errorf("the mounts below %s changed during the swap", m.path)
			}
		}

		for i, m := range mounts {
			err := unix.Unmount(m.path, unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW)
			// A path below an earlier swap was detached with it.
			if err != nil && (!errors.Is(err, unix.EINVAL) || !below(mounts[:i], m.path)) {
				return fmt.Errorf("detaching %s: %w", m.path, err)
			}
			if err := unix.MoveMount(clones[i], "", unix.AT_FDCWD, m.path, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
				return fmt.Errorf("mounting at %s: %w", m.path, err)
			}
			for _, c := range carry[i] {
				if err := unix.MoveMount(c.clone, "", c.at, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
					return fmt.Errorf("mounting %s again on the new mount at %s, which needs a restart of the container: %w", c.point, m.path, err)
				}
			}
		}
		return nil
	})
}

// inMountNS runs f on a thread of its own in the mount namespace at path, whose
// inode must be ino unless that is 0; errGone if it is not.
func inMountNS(path string, ino uint64, f func() error) error {
	errc := make(chan error, 1)
	go func() {
		// Never unlocked: the thread leaves the namespace of the healer, so the
		// runtime discards it when the goroutine exits, or wedges it if it is the
		// main thread, whose /proc/self then shows the other namespace.
		runtime.LockOSThread()
		errc <- func() error {
			// setns into a mount namespace fails for a thread sharing fs_struct.
			if err := unix.Unshare(unix.CLONE_FS); err != nil {
				return fmt.Errorf("unshare: %w", err)
			}
			fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
			if err != nil {
				return err
			}
			defer unix.Close(fd)
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil {
				return err
			}
			if ino != 0 && st.Ino != ino {
				return errGone
			}
			if err := unix.Setns(fd, unix.CLONE_NEWNS); err != nil {
				return fmt.Errorf("setns: %w", err)
			}
			return f()
		}()
	}()
	return <-errc
}

// childPoints returns the mount points of the mounts right below the mount id
// at path, but for those swapped on their own.
func childPoints(infos []mountInfo, id uint64, path string, swapped map[string]bool) []string {
	var points []string
	for _, mi := range infos {
		if mi.parent == id && strings.HasPrefix(mi.point, path+"/") && !swapped[mi.point] {
			points = append(points, mi.point)
		}
	}
	return points
}

func below(mounts []liveMount, path string) bool {
	return slices.ContainsFunc(mounts, func(m liveMount) bool { return strings.HasPrefix(path, m.path+"/") })
}

func closeFD(fd int) {
	if fd >= 0 {
		unix.Close(fd)
	}
}

// child is a mount below a swapped one: clone is its source with what is
// mounted below that, and at is its mount point in the new mount.
type child struct {
	point     string
	src       liveMount
	clone, at int
}

// cloneSource returns a detached recursive clone of the host path src.target.
func cloneSource(src liveMount) (int, error) {
	fd, err := unix.OpenTree(unix.AT_FDCWD, src.target, unix.OPEN_TREE_CLONE|unix.AT_RECURSIVE|unix.OPEN_TREE_CLOEXEC)
	if err != nil {
		return -1, err
	}
	if err := setMountAttr(fd, unix.AT_RECURSIVE, src); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// openMountPoint opens the mount point of c in parent, the new mount for path.
func openMountPoint(parent int, path string, c *child) error {
	missing := fmt.Errorf("%s is mounted below %s, and its mount point is missing in the new mount", c.point, path)
	at, err := unix.Openat2(parent, "."+strings.TrimPrefix(c.point, path), &unix.OpenHow{
		Flags: unix.O_PATH | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", missing, err)
	}
	c.at = at
	var cst, ast unix.Stat_t
	if err := errors.Join(unix.Fstat(c.clone, &cst), unix.Fstat(at, &ast)); err != nil {
		return err
	}
	// move_mount refuses a directory on a file and the other way round.
	if (cst.Mode&unix.S_IFMT == unix.S_IFDIR) != (ast.Mode&unix.S_IFMT == unix.S_IFDIR) {
		return missing
	}
	return nil
}

// cloneMount returns a detached clone of the new mount, or of the subPath in it.
func cloneMount(m liveMount) (int, error) {
	root, err := unix.Open(m.target, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	defer unix.Close(root)
	// A symlink in the volume must not lead the subPath out of it.
	src, err := unix.Openat2(root, filepath.Join(".", m.subPath), &unix.OpenHow{
		Flags: unix.O_PATH | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return -1, err
	}
	defer unix.Close(src)
	fd, err := unix.OpenTree(src, "", unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC|unix.AT_EMPTY_PATH)
	if err != nil {
		return -1, err
	}
	if err := setMountAttr(fd, 0, m); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// setMountAttr makes the clone fd read-only and sets its propagation as m asks.
func setMountAttr(fd int, flags uint, m liveMount) error {
	// kubelet mounts volumes rprivate unless asked otherwise, and a clone of a
	// shared mount joins its peer group.
	attr := unix.MountAttr{Propagation: unix.MS_PRIVATE}
	if m.propagation != nil {
		switch *m.propagation {
		case corev1.MountPropagationHostToContainer:
			attr.Propagation = unix.MS_SLAVE
		case corev1.MountPropagationBidirectional:
			attr.Propagation = 0
		}
	}
	if m.readOnly {
		attr.Attr_set = unix.MOUNT_ATTR_RDONLY
	}
	return unix.MountSetattr(fd, "", unix.AT_EMPTY_PATH|flags, &attr)
}

type mountInfo struct {
	id, parent uint64
	// root is the directory of the filesystem the mount shows, so a bind mount
	// tells where it was bound from.
	root  string
	point string
}

var mountinfoUnescaper = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)

func readMountinfo(proc string) ([]mountInfo, error) {
	b, err := os.ReadFile(proc + "/mountinfo")
	if err != nil {
		return nil, err
	}
	var infos []mountInfo
	for line := range strings.Lines(string(b)) {
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		id, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("mountinfo line %q: %w", line, err)
		}
		parent, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("mountinfo line %q: %w", line, err)
		}
		infos = append(infos, mountInfo{id: id, parent: parent, root: mountinfoUnescaper.Replace(f[3]), point: mountinfoUnescaper.Replace(f[4])})
	}
	return infos, nil
}

func nsInode(proc string) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Stat(proc+"/ns/mnt", &st); err != nil {
		return 0, err
	}
	return st.Ino, nil
}

// staleHandles counts the open files, working and root directories and mapped
// files that the processes in the mount namespace ns still hold on the dead
// mounts, and names the processes. errGone means pid left the namespace. Mount
// ids are global, so the ids from fdinfo and statx compare to those from
// mountinfo.
func staleHandles(pid int, ns uint64, dead map[uint64]bool, timeout time.Duration) (int, []string, error) {
	if ino, err := nsInode(fmt.Sprintf("/proc/%d", pid)); err != nil || ino != ns {
		return 0, nil, errGone
	}
	// statx on a hung FUSE mount can block despite AT_STATX_DONT_SYNC.
	p := newProber(timeout)
	p.stat = func(path string) (bool, error) {
		var st unix.Statx_t
		err := unix.Statx(unix.AT_FDCWD, path, unix.AT_STATX_DONT_SYNC, unix.STATX_MNT_ID, &st)
		if errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.ESTALE) {
			return true, nil
		}
		return err == nil && dead[st.Mnt_id], err
	}

	handles := 0
	var procs []string
	dirs, _ := filepath.Glob("/proc/[0-9]*")
	for _, dir := range dirs {
		if ino, err := nsInode(dir); err != nil || ino != ns {
			continue
		}
		n := 0
		// fdinfo never touches the file, so a dead mount cannot block it.
		fdinfos, _ := filepath.Glob(dir + "/fdinfo/*")
		for _, f := range fdinfos {
			if b, err := os.ReadFile(f); err == nil && dead[fdinfoMntID(b)] {
				n++
			}
		}
		maps, _ := filepath.Glob(dir + "/map_files/*")
		for _, link := range append([]string{dir + "/cwd", dir + "/root"}, maps...) {
			onDead, err := p.probe(link)
			if errors.Is(err, errHung) {
				// ponytail: the rest of this process likely hangs as well, so its count
				// is a lower bound; enough to know it still holds the dead mount.
				n++
				break
			}
			if onDead {
				n++
			}
		}
		if n > 0 {
			handles += n
			comm, _ := os.ReadFile(dir + "/comm")
			procs = append(procs, fmt.Sprintf("%s[%s]", bytes.TrimSpace(comm), filepath.Base(dir)))
		}
	}
	return handles, procs, nil
}

func fdinfoMntID(fdinfo []byte) uint64 {
	for line := range strings.Lines(string(fdinfo)) {
		if v, ok := strings.CutPrefix(line, "mnt_id:"); ok {
			id, _ := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
			return id
		}
	}
	return 0
}
