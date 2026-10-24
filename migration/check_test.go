package migration

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("testdata/cronicle-export.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func hasCode(r *Report, code string) bool {
	for _, f := range r.Findings {
		if f.Code == code {
			return true
		}
	}
	return false
}
func TestCheckActualExportPagesAndDeterministicOutput(t *testing.T) {
	data := fixture(t)
	first, err := Check(strings.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Counts["schedule"] != 2 || !first.ReadOnly || !hasCode(first, "inline_shell_adapter") {
		t.Fatalf("wrong report: %+v", first)
	}
	expected, _ := json.Marshal(first)
	lines := strings.Split(strings.TrimSpace(data), "\n")
	for i, j := 2, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	next, err := Check(strings.NewReader(strings.Join(lines, "\n")), nil)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := json.Marshal(next)
	if !bytes.Equal(expected, actual) {
		t.Fatal("record ordering changed output")
	}
}
func TestCheckFindsUnsupportedAndConversionRequirements(t *testing.T) {
	data := fixture(t)
	data = strings.Replace(data, `"timing":{"hours":[9],"minutes":[0]}`, `"detached":1,"timing":{"hours":[30],"minutes":[0]}`, 1)
	data = strings.Replace(data, `"category":"general"`, `"category":"missing"`, 1)
	data = strings.Replace(data, `"target":"all"`, `"target":"missing"`, 1)
	data = strings.Replace(data, `"timezone":"UTC"`, `"timezone":"not-a-time-zone"`, 1)
	data = strings.Replace(data, `"json":0}`, `"json":0,"unknown_param":1}`, 1)
	report, err := Check(strings.NewReader(data), strings.NewReader(`{"Storage":{"secret":"DO_NOT_ECHO"},"smtp_password":"ALSO_PRIVATE"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"detached", "invalid_timing", "invalid_timezone", "missing_category", "missing_target", "unknown_plugin_parameters", "unknown_configuration_fields"} {
		if !hasCode(report, code) {
			t.Errorf("missing finding %s", code)
		}
	}
	raw, _ := json.Marshal(report)
	for _, secret := range []string{"DO_NOT_ECHO", "ALSO_PRIVATE", "not-a-time-zone"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("input value leaked: %s", secret)
		}
	}
}
func TestCheckRejectsMalformedAndIncompleteExports(t *testing.T) {
	data := fixture(t)
	for name, input := range map[string]string{
		"JSON array": `[]`, "wrong version": "# Cronicle Data Export v2.0\n",
		"duplicate record":     data + "global/schedule - {}\n",
		"missing page":         strings.Replace(data, "global/schedule/0 -", "global/schedule/9 -", 1),
		"invalid count":        strings.Replace(data, `"length":2`, `"length":20`, 1),
		"null page":            strings.Replace(data, `"items":[]`, `"items":null`, 1),
		"invalid JSON":         data + "extra - {broken}\n",
		"duplicate JSON field": data + `extra - {"id":"a","id":"b"}`,
		"missing user":         strings.Replace(data, "users/fixture_user -", "users/missing -", 1),
		"huge page count":      strings.Replace(data, `"last_page":0`, `"last_page":2147483647`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Check(strings.NewReader(input), nil); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	if _, err := Check(strings.NewReader(data), strings.NewReader("null")); err == nil {
		t.Fatal("invalid config accepted")
	}
}

func TestCheckNeverExecutesPluginCommands(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "must-not-exist")
	command, _ := json.Marshal("/bin/sh -c 'touch " + marker + "'")
	data := strings.Replace(fixture(t), `"command":"bin/shell-plugin.js"`, `"command":`+string(command), 1)
	report, err := Check(strings.NewReader(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("checker executed plugin command")
	}
	encoded, _ := json.Marshal(report)
	if bytes.Contains(encoded, []byte(marker)) {
		t.Fatal("report included plugin command")
	}
}

func TestCheckSupportsNegativePageNumbersAndMultiplePages(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(fixture(t)), "\n")
	for i, line := range lines {
		key, raw, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		if key == "global/schedule" {
			var header map[string]interface{}
			_ = json.Unmarshal([]byte(raw), &header)
			header["first_page"] = -1
			data, _ := json.Marshal(header)
			lines[i] = key + " - " + string(data)
		}
		if key == "global/schedule/0" {
			var page struct {
				Type  string            `json:"type"`
				Items []json.RawMessage `json:"items"`
			}
			_ = json.Unmarshal([]byte(raw), &page)
			first, _ := json.Marshal(map[string]interface{}{"type": "list_page", "items": page.Items[:1]})
			second, _ := json.Marshal(map[string]interface{}{"type": "list_page", "items": page.Items[1:]})
			lines[i] = "global/schedule/-1 - " + string(first) + "\nglobal/schedule/0 - " + string(second)
		}
	}
	expected, err := Check(strings.NewReader(fixture(t)), nil)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := Check(strings.NewReader(strings.Join(lines, "\n")), nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(actual)
	b, _ := json.Marshal(expected)
	if !bytes.Equal(a, b) {
		t.Fatal("page splitting changed reconstructed results")
	}
}

func TestCheckRejectsNullListHeaderNumbers(t *testing.T) {
	for _, field := range []string{"first_page", "last_page", "length"} {
		t.Run(field, func(t *testing.T) {
			lines := strings.Split(fixture(t), "\n")
			for i, line := range lines {
				key, value, ok := strings.Cut(line, " - ")
				if !ok || key != "global/api_keys" {
					continue
				}
				var header map[string]interface{}
				if err := json.Unmarshal([]byte(value), &header); err != nil {
					t.Fatal(err)
				}
				header[field] = nil
				raw, err := json.Marshal(header)
				if err != nil {
					t.Fatal(err)
				}
				lines[i] = key + " - " + string(raw)
			}
			report, err := Check(strings.NewReader(strings.Join(lines, "\n")), nil)
			if err == nil || report != nil {
				t.Fatalf("null %s was treated as zero: report=%+v, err=%v", field, report, err)
			}
		})
	}
}
