//go:build linux || darwin

package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

// A real Go leader spawns a real Go child in its process group, then exits.
// Using our own executable avoids platform restrictions on protected binaries'
// environment reads, and tests recovery after a group leader no longer exists.
func TestRecoveryGroupChild(t *testing.T) {
	mode := os.Getenv("OWLCRON_RECOVERY_GROUP_CHILD")
	if mode == "" {
		return
	}
	path := os.Getenv("OWLCRON_RECOVERY_GROUP_PATH")
	if mode == "member" {
		if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
			os.Exit(2)
		}
		time.Sleep(30 * time.Second)
		return
	}
	if mode != "leader" {
		os.Exit(2)
	}
	exe, err := os.Executable()
	if err != nil {
		os.Exit(2)
	}
	cmd := exec.Command(exe, "-test.run=^TestRecoveryGroupChild$")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "OWLCRON_RECOVERY_GROUP_CHILD=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "OWLCRON_RECOVERY_GROUP_CHILD=member")
	if err = cmd.Start(); err != nil {
		os.Exit(2)
	}
	if err = os.WriteFile(path, []byte(strconv.Itoa(cmd.Process.Pid)), 0600); err != nil {
		os.Exit(2)
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if _, err = os.Stat(path + ".exit"); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	os.Exit(2)
}

func TestRecoveryReclaimsDescendantsAfterGroupLeaderExit(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "child-pid")
	token := fmt.Sprintf("orphan-group-%d", time.Now().UnixNano())
	cmd := exec.Command(exe, "-test.run=^TestRecoveryGroupChild$")
	cmd.Env = append(os.Environ(), "OWLCRON_RECOVERY_GROUP_CHILD=leader", "OWLCRON_RECOVERY_GROUP_PATH="+path, "OWLCRON_JOB_TOKEN="+token)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	leader := cmd.Process.Pid
	identity, err := processIdentity(leader)
	if err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-leader, syscall.SIGKILL); _ = cmd.Wait() }()
	var child int
	ready := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, e := os.ReadFile(path)
		if e == nil {
			child, _ = strconv.Atoi(string(raw))
		}
		if child > 1 {
			if _, e = os.Stat(path + ".ready"); e == nil {
				ready = true
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if child <= 1 || !ready {
		t.Fatal("leader did not start child")
	}
	childIdentity, err := processIdentity(child)
	if err != nil {
		t.Fatal(err)
	}
	group, err := syscall.Getpgid(child)
	if err != nil || group != leader {
		t.Fatalf("child group=%d want leader %d: %v", group, leader, err)
	}
	if err = os.WriteFile(path+".exit", []byte("exit"), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not exit")
	}
	if _, err = processIdentity(leader); !os.IsNotExist(err) {
		t.Fatalf("leader still present: %v", err)
	}
	job := &types.Job{PID: leader, ProcessIdentity: identity, ProcessToken: token + "-wrong"}
	if err = stopRecoveredJobProcess(job); err != nil {
		t.Fatal(err)
	}
	if got, e := processIdentity(child); e != nil || got != childIdentity {
		t.Fatalf("incorrect token killed or replaced descendant: %q %v", got, e)
	}
	job.ProcessToken = token
	if err = stopRecoveredJobProcess(job); err != nil {
		t.Fatal(err)
	}
	if _, err = processIdentity(child); !os.IsNotExist(err) {
		t.Fatalf("owned orphan descendant remains live: %v", err)
	}
}
