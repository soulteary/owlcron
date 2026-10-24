//go:build linux

package engine

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func processStat(pid int) ([]string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return nil, err
	}
	// comm may contain spaces and parentheses; the last closing parenthesis
	// precedes the fixed-layout fields beginning at state (field 3).
	end := bytes.LastIndexByte(raw, ')')
	if end < 0 {
		return nil, fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 20 {
		return nil, fmt.Errorf("short process stat")
	}
	return fields, nil
}
func processIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", os.ErrNotExist
	}
	fields, err := processStat(pid)
	if err != nil {
		return "", err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return "", os.ErrNotExist
	}
	return fmt.Sprintf("%s:%d:%s", strings.TrimSpace(string(boot)), pid, fields[19]), nil
}
func processGroupMembers(group int) ([]int, error) {
	if group < 0 {
		return nil, os.ErrNotExist
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var result []int
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil {
			continue
		}
		fields, e := processStat(pid)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		pg, e := strconv.Atoi(fields[2])
		if e != nil {
			return nil, e
		}
		if (group == 0 || pg == group) && fields[0] != "Z" && fields[0] != "X" {
			result = append(result, pid)
		}
	}
	return result, nil
}
func processHasRecoveryToken(pid int, token string) (bool, error) {
	if pid <= 0 {
		return false, os.ErrNotExist
	}
	if _, err := processIdentity(pid); err != nil {
		return false, err
	}
	f, err := os.Open(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	if err != nil {
		return false, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return false, err
	}
	if len(raw) > 1<<20 {
		return false, fmt.Errorf("process environment exceeds limit")
	}
	wanted := []byte("OWLCRON_JOB_TOKEN=" + token)
	for _, v := range bytes.Split(raw, []byte{0}) {
		if bytes.Equal(v, wanted) {
			return true, nil
		}
	}
	return false, nil
}
