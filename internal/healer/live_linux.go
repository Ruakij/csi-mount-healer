//go:build linux

package healer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
)

// swapMounts replaces what is mounted at each path in the mount namespace of pid
// with a clone of the new mount on the node, and returns the inode of that
// namespace and the ids of the replaced mounts.
func swapMounts(pid int, mounts []liveMount) (uint64, map[uint64]bool, error) {
	ns, err := nsInode(fmt.Sprintf("/proc/%d", pid))
	if err != nil {
		return 0, nil, err
	}
	// Mount points relative to the root of pid, which is the root of its namespace.
	infos, err := readMountinfo(pid)
	if err != nil {
		return 0, nil, err
	}
	dead := map[uint64]bool{}
	for _, m := range mounts {
		var id uint64
		for _, mi := range infos {
			switch {
			case mi.point == m.path:
				// The last one is on top.
				id = mi.id
			case strings.HasPrefix(mi.point, m.path+"/"):
				return 0, nil, fmt.Errorf("%s is mounted below %s and would be detached with it", mi.point, m.path)
			}
		}
		if id == 0 {
			return 0, nil, fmt.Errorf("nothing is mounted at %s", m.path)
		}
		dead[id] = true
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

	errc := make(chan error, 1)
	go func() {
		// Never unlocked: the thread leaves the namespace of the healer, so the
		// runtime discards it when the goroutine exits.
		runtime.LockOSThread()
		errc <- func() error {
			// setns into a mount namespace fails for a thread sharing fs_struct.
			if err := unix.Unshare(unix.CLONE_FS); err != nil {
				return fmt.Errorf("unshare: %w", err)
			}
			fd, err := unix.Open(fmt.Sprintf("/proc/%d/ns/mnt", pid), unix.O_RDONLY|unix.O_CLOEXEC, 0)
			if err != nil {
				return err
			}
			defer unix.Close(fd)
			var st unix.Stat_t
			if err := unix.Fstat(fd, &st); err != nil {
				return err
			}
			if st.Ino != ns {
				return errGone
			}
			if err := unix.Setns(fd, unix.CLONE_NEWNS); err != nil {
				return fmt.Errorf("setns: %w", err)
			}
			for i, m := range mounts {
				if err := unix.Unmount(m.path, unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW); err != nil {
					return fmt.Errorf("detaching %s: %w", m.path, err)
				}
				if err := unix.MoveMount(clones[i], "", unix.AT_FDCWD, m.path, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
					return fmt.Errorf("mounting at %s: %w", m.path, err)
				}
			}
			return nil
		}()
	}()
	return ns, dead, <-errc
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
	if err := unix.MountSetattr(fd, "", unix.AT_EMPTY_PATH, &attr); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

type mountInfo struct {
	id uint64
	// root is the directory of the filesystem the mount shows, so a bind mount
	// tells where it was bound from.
	root  string
	point string
}

var mountinfoUnescaper = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)

func readMountinfo(pid int) ([]mountInfo, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/mountinfo", pid))
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
		infos = append(infos, mountInfo{id: id, root: mountinfoUnescaper.Replace(f[3]), point: mountinfoUnescaper.Replace(f[4])})
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
