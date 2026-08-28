package main

import (
	"os"
	"testing"
)

func TestRunRejectsUnknownFlag(t *testing.T) {
	t.Parallel()

	originalArgs := os.Args
	t.Cleanup(func() { os.Args = originalArgs })
	os.Args = []string{"job-controller", "-unknown-flag"}
	if err := run(); err == nil {
		t.Fatal("run() error = nil, want flag parse error")
	}
}
