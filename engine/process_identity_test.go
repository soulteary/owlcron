//go:build linux || darwin

package engine

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestRecoveryProcessIdentityAndTokenBoundary(t *testing.T) {
	token := "identity-regression-exact-token"
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestRecoveryIdentityChild$", "--", "OWLCRON_JOB_TOKEN=argv-spoof")
	cmd.Env = append(os.Environ(), "OWLCRON_JOB_TOKEN="+token, "OWLCRON_IDENTITY_CHILD=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	pid := cmd.Process.Pid
	identity, err := processIdentity(pid)
	if err != nil || identity == "" {
		t.Fatalf("identity: %q %v", identity, err)
	}
	all, err := processGroupMembers(0)
	if err != nil {
		t.Fatal(err)
	}
	foundSelf := false
	for _, member := range all {
		if member == os.Getpid() {
			foundSelf = true
		}
	}
	if !foundSelf {
		t.Fatal("all-process enumeration omitted parent")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				got, err := processIdentity(pid)
				if err != nil || got != identity {
					t.Errorf("identity changed: %q %v", got, err)
					return
				}
				ok, err := processHasRecoveryToken(pid, token)
				if err != nil || !ok {
					t.Errorf("exact environment token missing: %v", err)
					return
				}
				for _, wrong := range []string{token + "-suffix", "argv-spoof"} {
					ok, err = processHasRecoveryToken(pid, wrong)
					if err != nil || ok {
						t.Errorf("incorrect token accepted %q: %v", wrong, err)
						return
					}
				}
				members, err := processGroupMembers(pid)
				if err != nil {
					t.Error(err)
					return
				}
				found := false
				for _, member := range members {
					if member == pid {
						found = true
					}
				}
				if !found {
					t.Error("live process missing from group")
				}
			}
		}()
	}
	wg.Wait()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if _, err := processIdentity(pid); !os.IsNotExist(err) {
		t.Errorf("exited PID error=%v want notexist", err)
	}
	if ok, err := processHasRecoveryToken(pid, token); !os.IsNotExist(err) || ok {
		t.Errorf("exited environment: %t %v", ok, err)
	}
}

func TestRecoveryProcessIdentityRejectsInvalidPID(t *testing.T) {
	for _, pid := range []int{-1, 0, 1 << 30} {
		if value, err := processIdentity(pid); err == nil {
			t.Errorf("invalid pid %d produced %s", pid, value)
		}
		if ok, err := processHasRecoveryToken(pid, fmt.Sprint(pid)); err == nil || ok {
			t.Errorf("invalid environment PID %d: %t %v", pid, ok, err)
		}
	}
}

func TestRecoveryIdentityChild(t *testing.T) {
	if os.Getenv("OWLCRON_IDENTITY_CHILD") != "1" {
		return
	}
	time.Sleep(30 * time.Second)
}

func TestRecoveryTokenDuringProcessExit(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for range 12 {
		cmd := exec.Command(exe, "-test.run=^TestRecoveryIdentityChild$")
		cmd.Env = append(os.Environ(), "OWLCRON_IDENTITY_CHILD=1", "OWLCRON_JOB_TOKEN=exit-regression-token")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		// Deliberately leave the child unreaped while querying the kernel. A zombie
		// must never be mistaken for a live process that prevents daemon recovery.
		deadline := time.Now().Add(time.Second)
		for {
			_, err = processHasRecoveryToken(cmd.Process.Pid, "exit-regression-token")
			if os.IsNotExist(err) {
				break
			}
			if err != nil {
				t.Errorf("token lookup during exit: %v", err)
				break
			}
			if time.Now().After(deadline) {
				t.Error("exited process remained live")
				break
			}
			time.Sleep(time.Millisecond)
		}
		_ = cmd.Wait()
	}
}
