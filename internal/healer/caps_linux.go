//go:build linux

package healer

import (
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// keptCaps are all the healer uses: unmounting and open_tree (CAP_SYS_ADMIN), the
// guard (CAP_LINUX_IMMUTABLE), and opening the directory underneath a mount for
// the guard, which the pod may own with a mode that locks root out
// (CAP_DAC_READ_SEARCH). Everything else it touches is owned by root: the kubelet
// directory, the CSI and CRI sockets.
var keptCaps = []uintptr{unix.CAP_SYS_ADMIN, unix.CAP_LINUX_IMMUTABLE, unix.CAP_DAC_READ_SEARCH}

// capsHeader and capsData stay at a fixed address while the kernel reads them.
var (
	capsHeader = unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	capsData   [2]unix.CapUserData
)

// DropPrivileges reduces every capability set of the process to keptCaps and
// sets no_new_privs. A privileged container starts with every capability and
// ignores capabilities.drop, so the binary drops them itself. Capabilities are
// per thread, so every change goes to all threads.
func DropPrivileges() error {
	keep := map[uintptr]bool{}
	for _, c := range keptCaps {
		keep[c] = true
		capsData[c/32].Effective |= 1 << (c % 32)
	}
	capsData[0].Permitted, capsData[1].Permitted = capsData[0].Effective, capsData[1].Effective

	// Needs CAP_SETPCAP, so before capset drops it.
	for c := uintptr(0); c < 64; c++ {
		if keep[c] {
			continue
		}
		if err := allThreads(unix.SYS_PRCTL, unix.PR_CAPBSET_DROP, c, 0); err != nil && !errors.Is(err, unix.EINVAL) {
			return fmt.Errorf("dropping capability %d from the bounding set: %w", c, err)
		}
	}
	if err := allThreads(unix.SYS_PRCTL, unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0); err != nil {
		return fmt.Errorf("clearing ambient capabilities: %w", err)
	}
	err := allThreads(unix.SYS_CAPSET, uintptr(unsafe.Pointer(&capsHeader)), uintptr(unsafe.Pointer(&capsData[0])), 0)
	runtime.KeepAlive(&capsHeader)
	if err != nil {
		return fmt.Errorf("setting capabilities: %w", err)
	}
	if err := allThreads(unix.SYS_PRCTL, unix.PR_SET_NO_NEW_PRIVS, 1, 0); err != nil {
		return fmt.Errorf("setting no_new_privs: %w", err)
	}
	return nil
}

func allThreads(trap, a1, a2, a3 uintptr) error {
	if _, _, errno := syscall.AllThreadsSyscall(trap, a1, a2, a3); errno != 0 {
		return errno
	}
	return nil
}
