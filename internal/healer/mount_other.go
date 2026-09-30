//go:build !linux

package healer

import (
	"errors"
	"runtime"
)

var errUnsupported = errors.New("csi-mount-healer is only supported on linux, not " + runtime.GOOS)

func stat(string) (bool, error) { return false, errUnsupported }

func detach(string) error { return errUnsupported }

func setImmutable(string, bool) error { return errUnsupported }

func DropPrivileges() error { return errUnsupported }
