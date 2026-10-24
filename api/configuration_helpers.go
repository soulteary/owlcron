package api

import (
	"encoding/json"
	"fmt"
	"github.com/soulteary/owlcron/storage"
	"io"
	"net/http"
	"sort"
)

// decodeStoredPatch merges a sparse update into its canonical record. Missing
// fields never disable or erase the rest of a plugin/category/group definition.
func (s *Server) decodeStoredPatch(r *http.Request, prefix string, target interface{}) (string, error) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	var req struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(raw, &req); err != nil {
		return "", err
	}
	if !isValidID(req.ID) {
		return "", fmt.Errorf("valid id is required")
	}
	data, err := s.engine.GetStorage().Get(r.Context(), prefix+":"+req.ID)
	if err != nil {
		return "", fmt.Errorf("record not found")
	}
	if err = json.Unmarshal(data, target); err != nil {
		return "", err
	}
	return req.ID, json.Unmarshal(raw, target)
}
func (s *Server) configurationChanged(key string) {
	data := s.loadScheduleData()
	s.engine.GetCommunication().BroadcastToSockets("update", map[string]interface{}{key: data[key]})
}
func (s *Server) deleteConfigurationRecord(r *http.Request, key, id string) error {
	if !isValidID(id) {
		return fmt.Errorf("valid id is required")
	}
	store := s.engine.GetStorage()
	if store == nil {
		return fmt.Errorf("storage not initialized")
	}
	if err := store.Delete(r.Context(), key+":"+id); err != nil {
		return err
	}
	return storage.UpdateList(r.Context(), store, key, func(items []interface{}) ([]interface{}, error) {
		out := []interface{}{}
		for _, item := range items {
			raw, _ := json.Marshal(item)
			var record struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(raw, &record) == nil && record.ID != id {
				out = append(out, item)
			}
		}
		return out, nil
	})
}
func (s *Server) configurationInUse(key, id string) bool {
	for _, event := range s.engine.GetScheduler().GetAllEvents() {
		switch key {
		case "plugins":
			if event.Plugin == id {
				return true
			}
		case "categories":
			if event.Category == id {
				return true
			}
		case "server_groups":
			if event.Target == id {
				return true
			}
		}
	}
	return false
}

const maxConfigurationPageSize = 10000

func recordPage[T any](r *http.Request, records []*T, id func(*T) string) map[string]interface{} {
	sort.Slice(records, func(i, j int) bool {
		return id(records[i]) < id(records[j])
	})
	offset, limit := parseOffsetLimit(r, 0, len(records))
	if offset < 0 {
		offset = 0
	}
	if offset > len(records) {
		offset = len(records)
	}
	if limit <= 0 || limit > maxConfigurationPageSize {
		limit = maxConfigurationPageSize
	}
	end := offset + min(limit, len(records)-offset)
	return map[string]interface{}{"rows": records[offset:end], "list": map[string]int{"length": len(records)}}
}
