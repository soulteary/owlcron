package engine

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func recoverySleep(t *testing.T) (*exec.Cmd, string, string) {
	t.Helper()
	token := fmt.Sprintf("recovery-test-%d", time.Now().UnixNano())
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestProcessRecoveryChild$")
	cmd.Env = append(os.Environ(), "OWLCRON_JOB_TOKEN="+token, "OWLCRON_PROCESS_RECOVERY_CHILD=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
		}
	})
	identity, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	return cmd, token, identity
}
func TestProcessRecoveryNeverSignalsReusedPID(t *testing.T) {
	cmd, token, _ := recoverySleep(t)
	if err := stopRecoveredJobProcess(&types.Job{PID: cmd.Process.Pid, ProcessIdentity: "different-process-start", ProcessToken: token}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(cmd.Process.Pid, 0); err != nil {
		t.Fatal("unrelated reused PID was signalled")
	}
}
func TestProcessRecoveryRequiresLegacyLivePIDVerification(t *testing.T) {
	cmd, _, _ := recoverySleep(t)
	if err := stopRecoveredJobProcess(&types.Job{PID: cmd.Process.Pid}); err == nil {
		t.Fatal("unverified live legacy PID silently accepted")
	}
	if err := syscall.Kill(cmd.Process.Pid, 0); err != nil {
		t.Fatal("unverified legacy process was signalled")
	}
}
func TestProcessRecoveryFindsExecBeforePIDPersistence(t *testing.T) {
	cmd, token, _ := recoverySleep(t)
	if err := stopRecoveredJobProcess(&types.Job{ProcessToken: token}); err != nil {
		t.Fatal(err)
	}
	// Reaping is owned by this test; the recovered process cannot execute again.
	if err := cmd.Wait(); err == nil {
		t.Fatal("recovery did not stop token-owned process")
	}
}
func TestProcessRecoveryProtectsDaemonProcessGroup(t *testing.T) {
	identity, err := processIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err = stopRecoveredJobProcess(&types.Job{PID: os.Getpid(), ProcessIdentity: identity}); err == nil {
		t.Fatal("daemon process group accepted for cleanup")
	}
}
func TestJobRecoveryTokenCannotBeOverriddenByParameters(t *testing.T) {
	e := jobTestEngine(t)
	job := &types.Job{ProcessToken: "guarded-token", Env: map[string]string{"OWLCRON_JOB_TOKEN": "bad-env"}, Params: map[string]interface{}{"OWLCRON_JOB_TOKEN": "bad-param"}}
	env := e.jobManager.buildEnv(job, nil)
	last := ""
	for _, item := range env {
		if len(item) >= 18 && item[:18] == "OWLCRON_JOB_TOKEN=" {
			last = item
		}
	}
	if last != "OWLCRON_JOB_TOKEN=guarded-token" {
		t.Fatalf("token overridden: %q", last)
	}
}

func TestProcessRecoveryChild(t *testing.T) {
	if os.Getenv("OWLCRON_PROCESS_RECOVERY_CHILD") == "1" {
		time.Sleep(60 * time.Second)
	}
}
