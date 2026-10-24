//go:build darwin

package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func processReadError(err error) error {
	if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) {
		return os.ErrNotExist
	}
	return err
}
func processIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", os.ErrNotExist
	}
	p, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		if errors.Is(err, unix.EIO) {
			return "", os.ErrNotExist
		}
		return "", processReadError(err)
	}
	if p == nil || int(p.Proc.P_pid) != pid || p.Proc.P_stat == 5 {
		return "", os.ErrNotExist
	}
	return fmt.Sprintf("%d:%d:%d", pid, p.Proc.P_starttime.Sec, p.Proc.P_starttime.Usec), nil
}
func processGroupMembers(group int) ([]int, error) {
	if group < 0 {
		return nil, os.ErrNotExist
	}
	name := "kern.proc.pgrp"
	args := []int{group}
	if group == 0 {
		name = "kern.proc.all"
		args = nil
	}
	ps, err := unix.SysctlKinfoProcSlice(name, args...)
	if err != nil {
		return nil, processReadError(err)
	}
	result := make([]int, 0, len(ps))
	for _, p := range ps {
		if p.Proc.P_stat != 5 {
			result = append(result, int(p.Proc.P_pid))
		}
	}
	return result, nil
}
func processHasRecoveryToken(pid int, token string) (owned bool, resultErr error) {
	defer func() {
		if resultErr != nil {
			if _, err := processIdentity(pid); os.IsNotExist(err) {
				owned = false
				resultErr = os.ErrNotExist
			}
		}
	}()
	if pid <= 0 {
		return false, os.ErrNotExist
	}
	if _, err := processIdentity(pid); err != nil {
		return false, err
	}
	var raw []byte
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		raw, err = unix.SysctlRaw("kern.procargs2", pid)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EIO) {
			break
		}
		if _, checkErr := processIdentity(pid); os.IsNotExist(checkErr) {
			return false, os.ErrNotExist
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		if _, identityErr := processIdentity(pid); os.IsNotExist(identityErr) {
			return false, os.ErrNotExist
		}
		return false, processReadError(err)
	}
	if len(raw) > 1<<20 {
		return false, fmt.Errorf("process arguments exceed limit")
	}
	if len(raw) < 4 {
		return false, fmt.Errorf("short process arguments")
	}
	// kern.procargs2 stores argc, executable path, padding, argv, then env.
	argc := int(binary.NativeEndian.Uint32(raw[:4]))
	if argc < 0 || argc > 65536 {
		return false, fmt.Errorf("invalid argument count")
	}
	data := raw[4:]
	end := bytes.IndexByte(data, 0)
	if end < 0 {
		return false, fmt.Errorf("missing executable terminator")
	}
	data = data[end+1:]
	for len(data) > 0 && data[0] == 0 {
		data = data[1:]
	}
	for i := 0; i < argc; i++ {
		end = bytes.IndexByte(data, 0)
		if end < 0 {
			return false, fmt.Errorf("missing argument terminator")
		}
		data = data[end+1:]
	}
	wanted := []byte("OWLCRON_JOB_TOKEN=" + token)
	for _, v := range bytes.Split(data, []byte{0}) {
		if bytes.Equal(v, wanted) {
			return true, nil
		}
	}
	return false, nil
}
