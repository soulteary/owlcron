package api

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/soulteary/owlcron/storage"
	"github.com/soulteary/owlcron/types"
)

func TestExpiredLogRetainsHistoryAndDoesNotExposeDeletionMarker(t *testing.T) {
	e, handler := securityServer(t)
	session := addSecurityUser(t, e, "retentionadmin", types.Privileges{"admin": true})
	job := &types.Job{ID: "expiredlog", Event: "event", Complete: true, TimeStart: 1, TimeEnd: 2}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.GetStorage().Put(ctx, "jobs:"+job.ID, raw); err != nil {
		t.Fatal(err)
	}
	if err := storage.HistoryUpsert(ctx, e.GetStorage(), job); err != nil {
		t.Fatal(err)
	}
	if err := e.GetStorage().Put(ctx, "job_logs:"+job.ID, []byte("sensitive log")); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteJobLog(job.ID); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"get_job_log", "get_live_job_log", "fetch_delete_job_log"} {
		response := securityRequest(t, handler, "/api/app/"+endpoint, `{"id":"expiredlog"}`, session)
		if response["code"] == float64(0) || response["log"] != nil {
			t.Fatalf("expired log exposed by %s: %#v", endpoint, response)
		}
	}
	history := securityRequest(t, handler, "/api/app/get_history", `{}`, session)
	if history["code"] != float64(0) || len(history["rows"].([]interface{})) != 1 {
		t.Fatalf("log expiry removed history: %#v", history)
	}
	details := securityRequest(t, handler, "/api/app/get_job_details", `{"id":"expiredlog"}`, session)
	if details["code"] != float64(0) {
		t.Fatalf("log expiry removed details: %#v", details)
	}
}
