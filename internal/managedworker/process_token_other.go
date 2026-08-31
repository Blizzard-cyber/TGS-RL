//go:build !darwin

package managedworker

import "errors"

func platformProcessToken(int) (string, error) {
	return "", errors.New("platform process identity is unavailable")
}

func platformProcessStopped(int) (bool, error) {
	return false, errors.New("platform process state is unavailable")
}
