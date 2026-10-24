package api

import (
	"context"
	"errors"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func TestServerManagementRejectsPrivateReplicaAndRemovingMaster(t *testing.T) {
	e, handler := securityServer(t)
	session := addSecurityUser(t, e, "serveradmin", types.Privileges{"admin": true})
	result := securityRequest(t, handler, "/api/app/add_server", `{"hostname":"unreachable-worker"}`, session)
	if result["code"] == float64(0) {
		t.Fatal("private storage worker addition reported success")
	}
	if _, err := e.GetStorage().Get(context.Background(), "servers:unreachable-worker"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed worker persisted: %v", err)
	}
	result = securityRequest(t, handler, "/api/app/remove_server", `{"hostname":"`+e.Hostname()+`"}`, session)
	if result["code"] == float64(0) {
		t.Fatal("active master can remove itself")
	}
}

func TestServerRestartRequestsRealLifecycleAction(t *testing.T) {
	e, handler := securityServer(t)
	session := addSecurityUser(t, e, "restartadmin", types.Privileges{"admin": true})
	result := securityRequest(t, handler, "/api/app/restart_server", `{"hostname":"`+e.Hostname()+`"}`, session)
	if result["code"] != float64(0) || !e.RestartRequested() {
		t.Fatalf("restart only reported success: %#v", result)
	}
	select {
	case <-e.ShutdownRequested():
	default:
		t.Fatal("restart did not stop lifecycle")
	}
}
