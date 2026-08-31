//go:build darwin

package managedworker

import (
	"fmt"

	"golang.org/x/sys/unix"
)

const darwinProcessStopped = 4

func platformProcessToken(pid int) (string, error) {
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	started := process.Proc.P_starttime
	return fmt.Sprintf("darwin:%d:%d:%d", pid, started.Sec, started.Usec), nil
}

func platformProcessStopped(pid int) (bool, error) {
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return false, err
	}
	return process.Proc.P_stat == darwinProcessStopped, nil
}
