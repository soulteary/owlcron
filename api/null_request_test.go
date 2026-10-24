package api

import (
	"github.com/soulteary/owlcron/types"
	"testing"
)

func TestNullRequestPreservesReadsWithoutAuthorizingMutations(t *testing.T) {
	eng, handler := securityServer(t)
	session := addSecurityUser(t, eng, "null-reader", types.Privileges{"create_events": true, "cat_limit": true, "cat_allowed": true})
	for _, suffix := range []string{"", "/v1"} {
		response := securityRequest(t, handler, "/api/app/status"+suffix, "null", session)
		if response["code"] != float64(0) || response["master"] != true {
			t.Fatalf("legacy null status rejected: %#v", response)
		}
		if response := securityRequest(t, handler, "/api/app/create_event"+suffix+"?category=allowed", "null", session); response["code"] == float64(0) {
			t.Fatal("query authorized an empty mutation body")
		}
	}
}
