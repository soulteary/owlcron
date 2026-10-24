package engine

import (
	"github.com/soulteary/owlcron/types"
	"regexp"
)

// CanAccess applies Cronicle category and group restrictions to an event/job.
func (e *Engine) CanAccess(privileges map[string]bool, category, target string) bool {
	if privileges["admin"] {
		return true
	}
	if privileges["cat_limit"] && !privileges["cat_"+category] {
		return false
	}
	if !privileges["grp_limit"] || privileges["grp_"+target] {
		return true
	}
	groups, err := e.LoadServerGroupsFromStorage()
	if err != nil {
		return false
	}
	// Named groups must be explicitly allowed. Hostnames may match an allowed group.
	if _, isGroup := groups[target]; isGroup {
		return false
	}
	for id, group := range groups {
		if !privileges["grp_"+id] || group.Regexp == "" {
			continue
		}
		matched, err := regexp.MatchString(group.Regexp, target)
		if err == nil && matched {
			return true
		}
	}
	return false
}

// ScopedState keeps scheduler metadata from exposing restricted event/job IDs.
func (e *Engine) ScopedState(privileges map[string]bool) *types.State {
	state := e.GetState()
	if privileges["admin"] || (!privileges["cat_limit"] && !privileges["grp_limit"]) {
		return state
	}
	events := e.GetScheduler().GetAllEvents()
	allowedEvent := func(id string) bool {
		event, ok := events[id]
		return ok && e.CanAccess(privileges, event.Category, event.Target)
	}
	for id := range state.Cursors {
		if !allowedEvent(id) {
			delete(state.Cursors, id)
		}
	}
	for id := range state.Robins {
		if !allowedEvent(id) {
			delete(state.Robins, id)
		}
	}
	for id := range state.JobCodes {
		if !allowedEvent(id) {
			delete(state.JobCodes, id)
		}
	}
	for id := range state.FlaggedJobs {
		job, ok := e.LookupJob(id)
		if !ok || !e.CanAccess(privileges, job.Category, job.Target) {
			delete(state.FlaggedJobs, id)
		}
	}
	return state
}
