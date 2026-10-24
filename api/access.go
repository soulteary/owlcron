package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/soulteary/owlcron/types"
)

// requestBodyParameters leaves the original body intact for the endpoint handler.
func requestBodyParameters(r *http.Request) (map[string]interface{}, error) {
	params := make(map[string]interface{})
	if r.Body != nil && r.Method != http.MethodGet && r.Method != http.MethodHead {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024+1))
		if err != nil {
			return nil, err
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(raw))
		if len(raw) > 1024*1024 {
			return nil, fmt.Errorf("request body too large")
		}
		if len(bytes.TrimSpace(raw)) > 0 {
			var err error
			params, err = decodeRequestParameters(raw)
			if err != nil {
				return nil, err
			}
		}
	}
	return params, nil
}

func requestParameters(r *http.Request) (map[string]interface{}, error) {
	params, err := requestBodyParameters(r)
	if err != nil {
		return nil, err
	}
	for key, values := range r.URL.Query() {
		if err := validateParameterName(key); err != nil {
			return nil, err
		}
		if len(values) > 0 {
			for _, value := range values[1:] {
				if value != values[0] {
					return nil, fmt.Errorf("conflicting query parameter: %s", key)
				}
			}
			if bodyValue, exists := params[key]; exists {
				// Keep JSON types for handlers that consume this map. Only equal
				// scalars can also be represented unambiguously in a query string.
				var scalar string
				switch value := bodyValue.(type) {
				case string:
					scalar = value
				case bool, float64:
					scalar = fmt.Sprint(value)
				default:
					return nil, fmt.Errorf("conflicting request parameter: %s", key)
				}
				if scalar != values[0] {
					return nil, fmt.Errorf("conflicting request parameter: %s", key)
				}
				continue
			}
			params[key] = values[0]
		}
	}
	return params, nil
}

// Struct decoding accepts case-insensitive field names, whereas map lookups do
// not. Scope selectors must use their documented spelling in both sources.
func validateParameterName(key string) error {
	for _, selector := range []string{"id", "ids", "category", "target"} {
		if key != selector && strings.EqualFold(key, selector) {
			return fmt.Errorf("request parameter must be spelled %s", selector)
		}
	}
	return nil
}

func decodeRequestParameters(raw []byte) (map[string]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("request body must be a JSON object")
	}
	params := make(map[string]interface{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid JSON request")
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid JSON request")
		}
		if err := validateParameterName(key); err != nil {
			return nil, err
		}
		if _, exists := params[key]; exists {
			return nil, fmt.Errorf("duplicate JSON parameter: %s", key)
		}
		var value interface{}
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("invalid JSON request")
		}
		params[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("invalid JSON request")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("invalid JSON request")
	}
	return params, nil
}

func routePrivilege(path string) (string, bool) {
	if strings.HasPrefix(path, "/api/user/admin_") {
		return "", true
	}
	name := strings.TrimPrefix(strings.TrimSuffix(path, "/v1"), "/api/app/")
	switch name {
	case "get_notifications", "retry_notification":
		return "", true
	case "create_category":
		return "create_categories", false
	case "update_category":
		return "update_categories", false
	case "delete_category":
		return "delete_categories", false
	case "create_event":
		return "create_events", false
	case "update_event", "update_job", "update_jobs":
		return "edit_events", false
	case "delete_event":
		return "delete_events", false
	case "run_event":
		return "run_events", false
	case "abort_event", "abort_job", "abort_jobs", "flush_event_queue":
		return "abort_events", false
	case "update_master_state":
		return "state_update", false
	case "get_api_keys", "get_api_key", "create_api_key", "update_api_key", "delete_api_key",
		"create_plugin", "update_plugin", "delete_plugin",
		"create_server_group", "update_server_group", "delete_server_group", "check_add_server", "add_server",
		"remove_server", "restart_server", "shutdown_server", "delete_job", "fetch_delete_job_log", "get_activity", "check_user_exists":
		return "", true
	}
	return "", strings.TrimSuffix(path, "/v1") == "/api/user/create"
}

// Cluster-wide mutations are processed by the lease owner so the authoritative
// scheduler and worker reservations cannot diverge. Session and self-service
// account operations use shared storage and remain available on every node.
func requiresMaster(path string) bool {
	path = strings.TrimSuffix(path, "/v1")
	if path == "/api/user/create" || path == "/api/user/admin_create" || path == "/api/user/admin_update" || path == "/api/user/admin_delete" {
		return true
	}
	if !strings.HasPrefix(path, "/api/app/") {
		return false
	}
	name := strings.TrimPrefix(path, "/api/app/")
	if name == "fetch_delete_job_log" || name == "retry_notification" {
		return true
	}
	for _, prefix := range []string{"create_", "update_", "delete_", "run_", "abort_", "flush_", "add_", "remove_", "restart_", "shutdown_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (s *Server) authorize(path string, next http.Handler) http.Handler {
	path = strings.TrimSuffix(path, "/v1")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := path[strings.LastIndex(path, "/")+1:]
		mutation := name == "fetch_delete_job_log" || name == "retry_notification"
		for _, prefix := range []string{"create_", "update_", "delete_", "run_", "abort_", "flush_", "add_", "remove_", "restart_", "shutdown_", "admin_create", "admin_update", "admin_delete"} {
			if strings.HasPrefix(name, prefix) {
				mutation = true
			}
		}
		if strings.HasPrefix(path, "/api/user/") && (name == "create" || name == "update" || name == "delete") {
			mutation = true
		}
		if mutation && r.Method != http.MethodPost {
			s.sendError(w, 1, "POST required for mutations")
			return
		}
		auth := GetAuthContext(r)
		privilege, admin := routePrivilege(path)
		if admin && (auth == nil || !auth.IsAdmin) {
			s.sendError(w, 1, "Admin privileges required")
			return
		}
		if privilege != "" && (auth == nil || (!auth.IsAdmin && !auth.Privileges[privilege])) {
			s.sendError(w, 1, "Privilege '"+privilege+"' required")
			return
		}
		if requiresMaster(path) && !s.engine.EnsureLeadership() {
			s.sendError(w, 1, "Current master server required")
			return
		}
		params, err := requestParameters(r)
		if err != nil {
			s.sendError(w, 1, err.Error())
			return
		}
		// These endpoints decode JSON directly. Query-only selectors must not
		// authorize a different object than the body the endpoint executes.
		switch name {
		case "create_event", "delete_event", "abort_event", "abort_job",
			"create_category", "update_category", "delete_category":
			params, err = requestBodyParameters(r)
			if err != nil {
				s.sendError(w, 1, err.Error())
				return
			}
		}
		if auth != nil {
			if err := s.authorizeObjects(path, auth, params); err != nil {
				s.sendError(w, 1, err.Error())
				return
			}
		}
		buffer := &scopedResponse{header: make(http.Header)}
		next.ServeHTTP(buffer, r)
		for key, values := range buffer.header {
			w.Header()[key] = values
		}
		if buffer.status == 0 {
			buffer.status = http.StatusOK
		}
		body := buffer.body.Bytes()
		if strings.Contains(buffer.header.Get("Content-Type"), "application/json") {
			var payload interface{}
			if json.Unmarshal(body, &payload) == nil {
				// Login/resume authenticate themselves; apply the same scopes to their initial payload.
				if auth == nil {
					if obj, ok := payload.(map[string]interface{}); ok && obj["code"] == float64(0) {
						if user, ok := obj["user"].(map[string]interface{}); ok {
							raw, _ := json.Marshal(user["privileges"])
							var p types.Privileges
							if json.Unmarshal(raw, &p) == nil {
								auth = &AuthContext{Privileges: p, IsAdmin: p["admin"]}
							}
						}
					}
				}
				if auth != nil && !auth.IsAdmin {
					payload = s.filterPayload(payload, auth.Privileges)
					payload = s.filterRoutePayload(path, payload, auth.Privileges)
					body, _ = json.Marshal(payload)
				}
			}
		}
		w.WriteHeader(buffer.status)
		_, _ = w.Write(body)
	})
}

func (s *Server) filterRoutePayload(path string, payload interface{}, privileges map[string]bool) interface{} {
	name := strings.TrimPrefix(strings.TrimSuffix(path, "/v1"), "/api/app/")
	prefix, limited := "", false
	switch name {
	case "list_categories", "get_categories":
		prefix, limited = "cat_", privileges["cat_limit"]
	case "list_server_groups":
		prefix, limited = "grp_", privileges["grp_limit"]
	case "list_servers", "get_servers":
		prefix, limited = "server", privileges["grp_limit"]
	}
	if !limited {
		return payload
	}
	allowed := func(value interface{}) bool {
		object, ok := value.(map[string]interface{})
		if !ok {
			return false
		}
		if prefix != "server" {
			return privileges[prefix+stringParam(object, "id")]
		}
		groupPrivileges := map[string]bool{}
		for key, value := range privileges {
			groupPrivileges[key] = value
		}
		groupPrivileges["cat_limit"] = false
		return s.engine.CanAccess(groupPrivileges, "", stringParam(object, "hostname"))
	}
	if object, ok := payload.(map[string]interface{}); ok {
		for _, key := range []string{"rows", "data", "categories", "server_groups", "servers"} {
			switch values := object[key].(type) {
			case []interface{}:
				filtered := []interface{}{}
				for _, value := range values {
					if allowed(value) {
						filtered = append(filtered, value)
					}
				}
				object[key] = filtered
			case map[string]interface{}:
				filtered := map[string]interface{}{}
				for id, value := range values {
					if allowed(value) {
						filtered[id] = value
					}
				}
				object[key] = filtered
			}
		}
	}
	return payload
}

type scopedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *scopedResponse) Header() http.Header { return w.header }
func (w *scopedResponse) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
func (w *scopedResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(data)
}

func stringParam(params map[string]interface{}, key string) string {
	value, _ := params[key].(string)
	return value
}

func (s *Server) authorizeObjects(path string, auth *AuthContext, params map[string]interface{}) error {
	if auth.LogJobID != "" && auth.LogJobID != stringParam(params, "id") {
		return fmt.Errorf("Log token access denied")
	}
	if auth.IsAdmin {
		return nil
	}
	name := strings.TrimPrefix(strings.TrimSuffix(path, "/v1"), "/api/app/")
	check := func(category, target string) error {
		if !s.engine.CanAccess(auth.Privileges, category, target) {
			return fmt.Errorf("Category or server group access denied")
		}
		return nil
	}
	switch name {
	case "create_event", "update_event":
		category, target := stringParam(params, "category"), stringParam(params, "target")
		if name == "update_event" {
			if old, exists := s.engine.GetScheduler().GetEvent(stringParam(params, "id")); exists {
				if _, specified := params["category"]; !specified {
					category = old.Category
				}
				if _, specified := params["target"]; !specified {
					target = old.Target
				}
			}
		}
		if err := check(category, target); err != nil {
			return err
		}
	}
	if strings.Contains(name, "event") && name != "create_event" && name != "list_events" {
		id := stringParam(params, "id")
		if event, ok := s.engine.GetScheduler().GetEvent(id); ok {
			if err := check(event.Category, event.Target); err != nil {
				return err
			}
		}
		if name == "abort_event" {
			for _, job := range s.engine.GetActiveJobs() {
				if job.Event == id {
					if err := check(job.Category, job.Target); err != nil {
						return err
					}
				}
			}
		}
	}
	if (strings.Contains(name, "job") || name == "get_log_watch_auth") && name != "list_jobs" && name != "get_active_jobs" {
		ids := []string{}
		if id := stringParam(params, "id"); id != "" {
			ids = append(ids, id)
		}
		if items, ok := params["ids"].([]interface{}); ok {
			for _, item := range items {
				if id, ok := item.(string); ok {
					ids = append(ids, id)
				}
			}
		}
		for _, id := range ids {
			job, ok := s.engine.LookupJob(id)
			if ok {
				if err := check(job.Category, job.Target); err != nil {
					return err
				}
			} else if auth.Privileges["cat_limit"] || auth.Privileges["grp_limit"] {
				return fmt.Errorf("Job not found or access denied")
			}
		}
	}
	if (name == "get_category" || name == "create_category" || name == "update_category" || name == "delete_category") && auth.Privileges["cat_limit"] && !auth.Privileges["cat_"+stringParam(params, "id")] {
		return fmt.Errorf("Category access denied")
	}
	if name == "get_server_group" && auth.Privileges["grp_limit"] && !auth.Privileges["grp_"+stringParam(params, "id")] {
		return fmt.Errorf("Server group access denied")
	}
	return nil
}

func (s *Server) filterPayload(value interface{}, privileges map[string]bool) interface{} {
	switch data := value.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(data))
		for _, item := range data {
			if filtered := s.filterPayload(item, privileges); filtered != nil {
				out = append(out, filtered)
			}
		}
		return out
	case map[string]interface{}:
		if category, hasCategory := data["category"].(string); hasCategory {
			target, _ := data["target"].(string)
			if !s.engine.CanAccess(privileges, category, target) {
				return nil
			}
		}
		for key, item := range data {
			switch key {
			case "queue", "event_queue", "eventQueue":
				if values, ok := item.(map[string]interface{}); ok && (privileges["cat_limit"] || privileges["grp_limit"]) {
					out := map[string]interface{}{}
					for id, value := range values {
						if event, exists := s.engine.GetScheduler().GetEvent(id); exists && s.engine.CanAccess(privileges, event.Category, event.Target) {
							out[id] = value
						}
					}
					data[key] = out
				}
			case "state":
				if privileges["cat_limit"] || privileges["grp_limit"] {
					data[key] = s.engine.ScopedState(privileges)
				}
			case "categories", "server_groups":
				prefix, limited := "cat_", privileges["cat_limit"]
				if key == "server_groups" {
					prefix, limited = "grp_", privileges["grp_limit"]
				}
				if limited {
					if items, ok := item.([]interface{}); ok {
						out := []interface{}{}
						for _, v := range items {
							if m, ok := v.(map[string]interface{}); ok && privileges[prefix+stringParam(m, "id")] {
								out = append(out, v)
							}
						}
						data[key] = out
					}
				}
			case "schedule", "rows", "jobs", "active_jobs", "job", "event", "data":
				if mapped, ok := item.(map[string]interface{}); ok && (key == "schedule" || key == "active_jobs" || key == "jobs") {
					out := map[string]interface{}{}
					for id, object := range mapped {
						if filtered := s.filterPayload(object, privileges); filtered != nil {
							out[id] = filtered
						}
					}
					data[key] = out
				} else {
					data[key] = s.filterPayload(item, privileges)
				}
			}
		}
		return data
	}
	return value
}
