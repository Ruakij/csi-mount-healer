//go:build !linux

package healer

import (
	"context"
	"errors"
	"runtime"
	"time"
)

var errUnsupported = errors.New("csi-mount-healer is only supported on linux, not " + runtime.GOOS)

func stat(string) (bool, error) { return false, errUnsupported }

func detach(string) error { return errUnsupported }

func stagingBinds() (map[string]bool, error) { return nil, errUnsupported }

func setImmutable(string, bool) error { return errUnsupported }

func DropPrivileges() error { return errUnsupported }

func swapMounts(int, []liveMount) (uint64, map[uint64]bool, error) {
	return 0, nil, errUnsupported
}

func staleHandles(int, uint64, map[uint64]bool, time.Duration) (int, []string, error) {
	return 0, nil, errUnsupported
}

func watchMounts(context.Context) <-chan struct{} { return nil }
