//go:build linux

package healer

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// fsImmutableFL is FS_IMMUTABLE_FL from linux/fs.h, which x/sys does not export.
const fsImmutableFL = 0x10

// stat reports whether path is the root of a mount. STATX_ATTR_MOUNT_ROOT also
// catches bind mounts from the same filesystem, which comparing st_dev with the
// parent would miss.
func stat(path string) (mountRoot bool, err error) {
	var st unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_BASIC_STATS, &st); err != nil {
		return false, err
	}
	if st.Attributes_mask&unix.STATX_ATTR_MOUNT_ROOT == 0 {
		return false, errors.New("kernel does not report STATX_ATTR_MOUNT_ROOT (needs 5.8+)")
	}
	return st.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0, nil
}

// detach lazily unmounts everything stacked on path.
func detach(path string) error {
	for {
		err := unix.Unmount(path, unix.MNT_DETACH)
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// setImmutable sets or clears FS_IMMUTABLE_FL on the directory underneath
// whatever is mounted on path. A non-recursive clone of the parent mount shows
// that directory without the mounts on top, and never touches a hung mount.
// A missing path is not an error: there is nothing left to guard.
func setImmutable(path string, on bool) error {
	tree, err := unix.OpenTree(unix.AT_FDCWD, filepath.Dir(path), unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(tree)
	fd, err := unix.Openat(tree, filepath.Base(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	flags, err := unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
	if err != nil {
		return err
	}
	want := flags &^ fsImmutableFL
	if on {
		want |= fsImmutableFL
	}
	if want == flags {
		return nil
	}
	return unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, int(want))
}

// stagingBinds lists the mount points whose top mount is a bind of a bare
// staging directory: published while the staging mount was gone, so what is
// written there lands on the node disk. A bind of a live staging mount shows
// the root of the staged filesystem instead.
func stagingBinds() (map[string]bool, error) {
	infos, err := readMountinfo(os.Getpid())
	if err != nil {
		return nil, err
	}
	binds := map[string]bool{}
	for _, m := range infos {
		binds[m.point] = strings.Contains(m.root, "/plugins/kubernetes.io/csi/") && strings.HasSuffix(m.root, "/globalmount")
	}
	return binds, nil
}
