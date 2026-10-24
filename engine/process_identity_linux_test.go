//go:build linux

package engine

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestLinuxProcessReadError(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ESRCH, syscall.ENOENT} {
		err := &os.PathError{Op: "open", Path: "/proc/123/environ", Err: errno}
		if !os.IsNotExist(processReadError(err)) {
			t.Fatalf("%v did not become not-exist", errno)
		}
	}
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EPERM, syscall.EMFILE, syscall.EIO} {
		err := &os.PathError{Op: "read", Path: "/proc/123/environ", Err: errno}
		if normalized := processReadError(err); !errors.Is(normalized, errno) || os.IsNotExist(normalized) {
			t.Fatalf("live process error hidden: %v", normalized)
		}
	}
	if processReadError(nil) != nil {
		t.Fatal("nil error changed")
	}
}
