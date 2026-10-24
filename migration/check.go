// Package migration inspects Cronicle exports without opening OwlCron storage.
package migration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/soulteary/owlcron/api"
	"github.com/soulteary/owlcron/types"
)

const maxExportBytes = 32 << 20

var lists = []string{"api_keys", "categories", "plugins", "schedule", "server_groups", "servers", "users"}

type Finding struct {
	Section string `json:"section"`
	Item    int    `json:"item,omitempty"`
	Status  string `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
type Report struct {
	Format   string         `json:"format"`
	ReadOnly bool           `json:"read_only"`
	Counts   map[string]int `json:"counts"`
	Findings []Finding      `json:"findings"`
}

func (r *Report) add(section string, item int, status, code, message string) {
	r.Findings = append(r.Findings, Finding{section, item, status, code, message})
}
func object(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("expected JSON object")
	}
	value := map[string]json.RawMessage{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid JSON object")
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid JSON object")
		}
		if _, exists := value[key]; exists {
			return nil, fmt.Errorf("duplicate JSON field")
		}
		var field json.RawMessage
		if decoder.Decode(&field) != nil {
			return nil, fmt.Errorf("invalid JSON object")
		}
		value[key] = field
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("invalid JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("invalid JSON object")
	}
	return value, nil
}
func stringValue(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }

// Check reads the actual KEY - JSON export format emitted by Cronicle's
// bin/storage-cli.js. Findings contain no raw configuration or record values.
func Check(input io.Reader, config io.Reader) (*Report, error) {
	raw, err := io.ReadAll(io.LimitReader(input, maxExportBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read export")
	}
	if len(raw) > maxExportBytes {
		return nil, fmt.Errorf("export exceeds 32 MiB")
	}
	records := map[string]json.RawMessage{}
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 4096), maxExportBytes)
	line := 0
	header := false
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "# Cronicle Data Export v1.0" {
			header = true
		}
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		key, value, ok := strings.Cut(text, " - ")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid export record at line %d", line)
		}
		if _, exists := records[key]; exists {
			return nil, fmt.Errorf("duplicate export record at line %d", line)
		}
		if _, err := object([]byte(value)); err != nil {
			return nil, fmt.Errorf("invalid export object at line %d", line)
		}
		records[key] = json.RawMessage(value)
	}
	if scanner.Err() != nil {
		return nil, fmt.Errorf("read export line")
	}
	if !header {
		return nil, fmt.Errorf("missing Cronicle Data Export v1.0 header")
	}
	report := &Report{Format: "owlcron-migration-check-v1", ReadOnly: true, Counts: map[string]int{}, Findings: []Finding{}}
	entries := map[string][]json.RawMessage{}
	consumed := map[string]bool{}
	for _, name := range lists {
		key := "global/" + name
		var list struct {
			First  *int   `json:"first_page"`
			Last   *int   `json:"last_page"`
			Length *int   `json:"length"`
			Type   string `json:"type"`
		}
		raw, ok := records[key]
		if !ok {
			return nil, fmt.Errorf("missing required list %s", name)
		}
		consumed[key] = true
		if json.Unmarshal(raw, &list) != nil || list.First == nil || list.Last == nil || list.Length == nil || list.Type != "list" {
			return nil, fmt.Errorf("invalid list header %s", name)
		}
		first, last, length := *list.First, *list.Last, *list.Length
		if last < first || length < 0 || uint64(last)-uint64(first) >= uint64(len(records)) {
			return nil, fmt.Errorf("invalid list header %s", name)
		}
		// Lists can grow toward negative page numbers. The unsigned span
		// check above also rejects signed overflow from malicious headers.
		for offset := 0; offset <= int(uint64(last)-uint64(first)); offset++ {
			page := first + offset
			pageKey := key + "/" + strconv.Itoa(page)
			data, ok := records[pageKey]
			if !ok {
				return nil, fmt.Errorf("missing page in list %s", name)
			}
			consumed[pageKey] = true
			var value struct {
				Items []json.RawMessage `json:"items"`
			}
			var pageFields map[string]json.RawMessage
			_ = json.Unmarshal(data, &pageFields)
			if json.Unmarshal(data, &value) != nil || pageFields["items"] == nil || value.Items == nil {
				return nil, fmt.Errorf("invalid page in list %s", name)
			}
			for _, item := range value.Items {
				if _, err := object(item); err != nil {
					return nil, fmt.Errorf("invalid item in list %s", name)
				}
			}
			entries[name] = append(entries[name], value.Items...)
		}
		if len(entries[name]) != length {
			return nil, fmt.Errorf("item count mismatch in list %s", name)
		}
		report.Counts[name] = length
	}
	for key := range records {
		if !consumed[key] && !strings.HasPrefix(key, "users/") {
			report.add("export", 0, "unsupported", "unknown_storage_records", "Export contains storage records outside the supported Cronicle core lists.")
			break
		}
	}
	ids := map[string]map[string]bool{}
	for _, name := range lists {
		ids[name] = map[string]bool{}
		for _, raw := range entries[name] {
			obj, _ := object(raw)
			field := "id"
			if name == "servers" {
				field = "hostname"
			}
			if name == "users" {
				field = "username"
			}
			id := stringValue(obj[field])
			if id == "" || ids[name][id] {
				return nil, fmt.Errorf("missing or duplicate identifier in list %s", name)
			}
			ids[name][id] = true
		}
	}
	for _, raw := range entries["users"] {
		obj, _ := object(raw)
		name := stringValue(obj["username"])
		var key strings.Builder
		for _, ch := range strings.ToLower(name) {
			if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_' {
				key.WriteRune(ch)
			}
		}
		data, exists := records["users/"+key.String()]
		if !exists {
			return nil, fmt.Errorf("missing user detail record")
		}
		detail, _ := object(data)
		if stringValue(detail["username"]) != name {
			return nil, fmt.Errorf("user detail does not match list")
		}
	}
	report.add("export", 0, "requires_conversion", "storage_format", "Convert reviewed core records to OwlCron's storage format; this export cannot be passed directly to OwlCron import.")
	for _, name := range []string{"users", "api_keys", "servers"} {
		if len(entries[name]) > 0 {
			report.add(name, 0, "requires_conversion", "recreate_identity", "Recreate credentials or node identities and privileges in OwlCron; exported authentication data is not imported or verified.")
		}
	}
	for index, raw := range entries["categories"] {
		var v types.Category
		if json.Unmarshal(raw, &v) != nil || api.ValidateCategory(&v) != nil {
			report.add("categories", index+1, "unsupported", "invalid_category", "Category does not satisfy OwlCron validation.")
		}
	}
	for index, raw := range entries["server_groups"] {
		var v types.ServerGroup
		if json.Unmarshal(raw, &v) != nil || api.ValidateServerGroup(&v) != nil {
			report.add("server_groups", index+1, "unsupported", "invalid_group", "Group fields or regular expression do not satisfy OwlCron validation.")
		}
	}
	for index, raw := range entries["plugins"] {
		var p types.Plugin
		obj, _ := object(raw)
		if json.Unmarshal(raw, &p) != nil || api.ValidatePlugin(&p) != nil {
			report.add("plugins", index+1, "unsupported", "invalid_plugin", "Plugin does not satisfy OwlCron validation.")
			continue
		}
		if strings.Contains(p.Command, "shell-plugin.js") {
			report.add("plugins", index+1, "requires_conversion", "inline_shell_adapter", "Cronicle's inline shell adapter is not bundled. Review and move scripts to fixed files before configuring owlcron-script.")
		} else {
			report.add("plugins", index+1, "requires_conversion", "plugin_runtime_review", "Verify the command, runtime, uid/gid, environment, argument parsing and JSON-line completion contract on every target node; no command was executed.")
		}
		if unknownFields(obj, reflect.TypeOf(p)) {
			report.add("plugins", index+1, "requires_conversion", "extra_fields", "Plugin contains fields outside the OwlCron schema; review them explicitly.")
		}
	}
	for index, raw := range entries["schedule"] {
		var event types.Event
		obj, _ := object(raw)
		number := index + 1
		if json.Unmarshal(raw, &event) != nil {
			report.add("schedule", number, "unsupported", "invalid_event", "Event fields cannot be decoded using the OwlCron schema.")
			continue
		}
		if event.Detached.Bool() {
			report.add("schedule", number, "unsupported", "detached", "Detached execution is unsupported.")
		}
		validTiming := api.ValidateTiming(event.Timing) == nil
		if !validTiming {
			report.add("schedule", number, "unsupported", "invalid_timing", "Event timing is outside OwlCron's supported ranges.")
		}
		validTimezone := true
		if event.Timezone != "" {
			_, err := time.LoadLocation(event.Timezone)
			validTimezone = err == nil
		}
		if !validTimezone {
			report.add("schedule", number, "unsupported", "invalid_timezone", "Event time zone is not available to the checker runtime.")
		}
		validEvent := api.ValidateEvent(&event) == nil
		if !validEvent && !event.Detached.Bool() && validTiming && validTimezone {
			report.add("schedule", number, "unsupported", "event_validation", "Event timing, time zone, identifiers or limits do not satisfy OwlCron validation.")
		}
		if !ids["plugins"][event.Plugin] {
			report.add("schedule", number, "unsupported", "missing_plugin", "Event references a plugin absent from this export.")
		}
		if event.Category != "" && !ids["categories"][event.Category] {
			report.add("schedule", number, "unsupported", "missing_category", "Event references a category absent from this export.")
		}
		if event.Target != "local" && !ids["server_groups"][event.Target] && !ids["servers"][event.Target] {
			report.add("schedule", number, "requires_conversion", "missing_target", "Resolve the target to an OwlCron server group or registered hostname.")
		}
		if event.CWD != "" || event.UID != "" || event.GID != "" {
			report.add("schedule", number, "requires_conversion", "event_execution_context", "Execution context is inherited from the plugin; move event-level command context into reviewed plugin configuration.")
		}
		if unknownFields(obj, reflect.TypeOf(event)) {
			report.add("schedule", number, "requires_conversion", "extra_fields", "Event contains fields outside the OwlCron schema; review them explicitly.")
		}
		if event.Params != nil {
			for _, pluginRaw := range entries["plugins"] {
				var p types.Plugin
				_ = json.Unmarshal(pluginRaw, &p)
				if p.ID != event.Plugin {
					continue
				}
				allowed := map[string]bool{}
				for _, param := range p.Params {
					if param != nil {
						allowed[param.ID] = true
					}
				}
				for key := range event.Params {
					if !allowed[key] {
						report.add("schedule", number, "requires_conversion", "unknown_plugin_parameters", "Event supplies parameters outside its plugin schema.")
						break
					}
				}
			}
		}
		if validEvent {
			report.add("schedule", number, "supported", "scheduling_subset", "Scheduled or on-demand timing is inspected against OwlCron's supported schema; runtime and migration readiness still require review.")
		}
	}
	if config != nil {
		raw, err := io.ReadAll(io.LimitReader(config, maxExportBytes+1))
		if err != nil || len(raw) > maxExportBytes {
			return nil, fmt.Errorf("read configuration")
		}
		obj, err := object(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid configuration object")
		}
		var typed types.Config
		if json.Unmarshal(raw, &typed) != nil {
			report.add("configuration", 0, "unsupported", "configuration_types", "Configuration values do not match the OwlCron schema.")
		}
		report.add("configuration", 0, "requires_conversion", "independent_configuration", "Create an OwlCron configuration and new cluster identities separately; the export does not contain configuration.")
		if unknownFields(obj, reflect.TypeOf(types.Config{})) {
			report.add("configuration", 0, "unsupported", "unknown_configuration_fields", "Configuration contains fields outside OwlCron's schema; do not copy it unchanged.")
		}
	}
	sort.SliceStable(report.Findings, func(i, j int) bool {
		a, b := report.Findings[i], report.Findings[j]
		if a.Section != b.Section {
			return a.Section < b.Section
		}
		if a.Item != b.Item {
			return a.Item < b.Item
		}
		return a.Code < b.Code
	})
	return report, nil
}
func unknownFields(obj map[string]json.RawMessage, t reflect.Type) bool {
	known := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		known[strings.Split(t.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	for key := range obj {
		if !known[key] {
			return true
		}
	}
	return false
}
