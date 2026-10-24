package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/soulteary/owlcron/types"
)

func TestJobLogBoundedPagesAndSafeFetchDelete(t *testing.T) {
	e, handler := securityServer(t)
	session := addSecurityUser(t, e, "logadmin", types.Privileges{"admin": true})
	id := "pagedlog"
	job, _ := json.Marshal(&types.Job{ID: id, Complete: true, Event: "event"})
	ctx := context.Background()
	if err := e.GetStorage().Put(ctx, "jobs:"+id, job); err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("a", 65535) + "日志" + strings.Repeat("z", 5000)
	if err := e.GetStorage().Put(ctx, "job_logs:"+id, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	var collected []byte
	for offset := 0; ; {
		resp := securityRequest(t, handler, "/api/app/get_job_log", fmt.Sprintf(`{"id":%q,"offset":%d}`, id, offset), session)
		if resp["code"] != float64(0) {
			t.Fatalf("page: %#v", resp)
		}
		data, err := base64.StdEncoding.DecodeString(resp["log_base64"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > 65536 {
			t.Fatalf("unbounded page %d", len(data))
		}
		collected = append(collected, data...)
		next := int(resp["next_offset"].(float64))
		if resp["eof"] == true {
			break
		}
		if next <= offset {
			t.Fatal("no progress")
		}
		offset = next
	}
	if string(collected) != payload {
		t.Fatal("paged byte content changed")
	}
	resp := securityRequest(t, handler, "/api/app/fetch_delete_job_log", `{"id":"pagedlog"}`, session)
	if resp["code"] == float64(0) {
		t.Fatal("partial destructive fetch succeeded")
	}
	resp = securityRequest(t, handler, "/api/app/get_job_log", `{"id":"pagedlog","offset":65535,"limit":6}`, session)
	if resp["code"] != float64(0) || resp["log"] != "日志" {
		t.Fatalf("log lost after refused deletion: %#v", resp)
	}
	for _, params := range []string{`"offset":-1`, `"offset":1.5`, `"limit":0`, `"limit":1048577`, `"offset":"bad"`} {
		resp = securityRequest(t, handler, "/api/app/get_job_log", `{"id":"pagedlog",`+params+`}`, session)
		if resp["code"] == float64(0) {
			t.Fatalf("invalid range accepted: %s", params)
		}
	}
	token := makeJobLogToken(e, id)
	r := httptest.NewRequest(http.MethodGet, "/api/app/get_job_log?id="+id+"&t="+token+"&offset=65535&limit=6", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Body.String() != "日志" || w.Header().Get("X-Log-Next-Offset") != "65541" || w.Header().Get("X-Log-EOF") != "false" {
		t.Fatalf("GET page metadata: %v %q", w.Header(), w.Body.String())
	}
	r = httptest.NewRequest(http.MethodGet, "/api/app/get_job_log?id="+id+"&t="+token+"&stream=1&download=1", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Body.String() != payload || w.Header().Get("Content-Length") != fmt.Sprint(len(payload)) || !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("streamed download incomplete: bytes=%d header=%v", w.Body.Len(), w.Header())
	}

}

func TestStreamJobLogFreezesLengthAndBoundsReads(t *testing.T) {
	payload := []byte(strings.Repeat("日志", 30000))
	offset := int64(0)
	calls := 0
	w := httptest.NewRecorder()
	started, err := streamJobLog(w, httptest.NewRequest(http.MethodGet, "/", nil), "job", func(start, limit int64) ([]byte, int64, error) {
		calls++
		if start != offset || limit > defaultLogPageSize {
			t.Fatalf("range %d %d expected %d", start, limit, offset)
		}
		end := start + limit
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}
		offset = end
		// Later metadata reports growth, which must not extend the snapshot.
		total := int64(len(payload))
		if calls > 1 {
			total += 1000
		}
		return payload[start:end], total, nil
	})
	if !started || err != nil || calls < 2 || w.Header().Get("Content-Length") != fmt.Sprint(len(payload)) || !strings.EqualFold(w.Body.String(), string(payload)) {
		t.Fatalf("stream result started=%v err=%v calls=%d size=%d", started, err, calls, w.Body.Len())
	}
}
func TestStreamJobLogStopsOnReadFailureOrCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			calls := 0
			started, err := streamJobLog(w, r, "job", func(offset, limit int64) ([]byte, int64, error) {
				calls++
				if calls == 1 {
					if cancel {
						stop()
					}
					return []byte("abc"), 10, nil
				}
				return nil, 0, io.ErrUnexpectedEOF
			})
			if !started || err == nil || w.Body.Len() >= 10 || w.Header().Get("Content-Length") != "10" {
				t.Fatalf("failed stream: %v %v %d", started, err, w.Body.Len())
			}
		})
	}
}

type failedLogWriter struct {
	header http.Header
	writes int
}

func (w *failedLogWriter) Header() http.Header       { return w.header }
func (w *failedLogWriter) WriteHeader(int)           {}
func (w *failedLogWriter) Write([]byte) (int, error) { w.writes++; return 0, io.ErrClosedPipe }
func TestStreamJobLogStopsOnWriteFailure(t *testing.T) {
	w := &failedLogWriter{header: make(http.Header)}
	calls := 0
	started, err := streamJobLog(w, httptest.NewRequest(http.MethodGet, "/", nil), "job", func(offset, limit int64) ([]byte, int64, error) { calls++; return []byte("abc"), 10, nil })
	if !started || !errors.Is(err, io.ErrClosedPipe) || calls != 1 || w.writes != 1 {
		t.Fatalf("write failure continued: %v %v %d %d", started, err, calls, w.writes)
	}
}

type deadlineLogWriter struct{ failedLogWriter }

func (w *deadlineLogWriter) SetWriteDeadline(time.Time) error { return context.DeadlineExceeded }
func TestStreamJobLogStopsOnDeadlineFailure(t *testing.T) {
	w := &deadlineLogWriter{failedLogWriter{header: make(http.Header)}}
	started, err := streamJobLog(w, httptest.NewRequest(http.MethodGet, "/", nil), "job", func(offset, limit int64) ([]byte, int64, error) { return []byte("abc"), 3, nil })
	if !started || !errors.Is(err, context.DeadlineExceeded) || w.writes != 0 {
		t.Fatalf("deadline ignored: %v %v writes=%d", started, err, w.writes)
	}
}
