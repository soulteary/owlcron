package engine

import (
	"github.com/soulteary/owlcron/types"
	"strings"
	"testing"
)

func TestCronicle135HTMLReportsAreSanitized(t *testing.T) {
	input := `<p class="ignored" onclick="bad()">safe <b>bold</b></p><script>alert(1)</script><svg><a href="javascript:bad()">bad</a></svg><a href="&#106;avascript:bad()">unsafe</a><a href="https://example.test" target="_blank">safe link</a><div style="background:url(javascript:bad())">text</div>`
	output := sanitizeReportHTML(input)
	if strings.Contains(output, "script") || strings.Contains(output, "onclick") || strings.Contains(output, "style=") || strings.Contains(output, "javascript:") || strings.Contains(output, "<svg") {
		t.Fatalf("unsafe markup retained: %s", output)
	}
	if !strings.Contains(output, "<b>bold</b>") || !strings.Contains(output, `rel="noopener noreferrer"`) {
		t.Fatalf("safe formatting missing: %s", output)
	}
	job := &types.Job{HTML: map[string]interface{}{"content": input}, Table: map[string]interface{}{"rows": []interface{}{[]interface{}{`<img src=x onerror=bad()>`, float64(42)}}}}
	sanitizeJobReports(job)
	if strings.Contains(job.HTML.(map[string]interface{})["content"].(string), "onclick") {
		t.Fatal("job report not sanitized")
	}
	row := job.Table.(map[string]interface{})["rows"].([]interface{})[0].([]interface{})
	if strings.Contains(row[0].(string), "onerror") || row[1] != float64(42) {
		t.Fatal("table sanitization corrupt or unsafe")
	}
}
func TestDisabledChainIsNotQueued(t *testing.T) {
	e := jobTestEngine(t)
	event := &types.Event{ID: "disabledchain", Title: "Disabled", Enabled: false, Queue: true}
	if err := e.scheduler.AddEvent(event); err != nil {
		t.Fatal(err)
	}
	if !e.EnsureLeadership() {
		t.Fatal("no leadership")
	}
	if err := e.scheduler.ChainReaction(&types.Job{ID: "jsource", Event: "source"}, event.ID); err != nil {
		t.Fatal(err)
	}
	if e.GetEventQueues()[event.ID] != 0 {
		t.Fatal("disabled chain event entered queue")
	}
}
