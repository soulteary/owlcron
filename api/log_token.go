package api

import (
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/engine"
)

// A log token is a short-lived bearer credential scoped to one read-only log.
func makeJobLogToken(eng *engine.Engine, id string) string {
	secret := eng.GetConfig().SecretKey
	if len(secret) < 32 || strings.HasPrefix(secret, "CHANGE_ME") {
		return ""
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	expires := strconv.FormatInt(time.Now().Unix()+300, 10)
	nonce := hex.EncodeToString(buf)
	signature := computeHMAC(secret, "job-log\n"+id+"\n"+expires+"\n"+nonce)
	return expires + "." + nonce + "." + signature
}

func authenticateLogToken(eng *engine.Engine, r *http.Request) (*AuthContext, error) {
	path := strings.TrimSuffix(r.URL.Path, "/v1")
	if r.Method != http.MethodGet || (path != "/api/app/get_job_log" && path != "/api/app/get_live_job_log") {
		return nil, fmt.Errorf("log token cannot authorize this endpoint")
	}
	id, token := r.URL.Query().Get("id"), r.URL.Query().Get("t")
	parts := strings.Split(token, ".")
	if id == "" || len(parts) != 3 {
		return nil, fmt.Errorf("invalid log token")
	}
	expires, err := strconv.ParseInt(parts[0], 10, 64)
	now := time.Now().Unix()
	if err != nil || expires <= now || expires > now+300 {
		return nil, fmt.Errorf("expired log token")
	}
	secret := eng.GetConfig().SecretKey
	if len(secret) < 32 || strings.HasPrefix(secret, "CHANGE_ME") {
		return nil, fmt.Errorf("log token authentication unavailable")
	}
	expected := computeHMAC(secret, "job-log\n"+id+"\n"+parts[0]+"\n"+parts[1])
	if !hmac.Equal([]byte(parts[2]), []byte(expected)) {
		return nil, fmt.Errorf("invalid log token")
	}
	return &AuthContext{LogJobID: id, Privileges: map[string]bool{}}, nil
}
