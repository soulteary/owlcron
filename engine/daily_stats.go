package engine

import "time"

// Cronicle's day event uses the daemon's local timezone. Persisting its midnight
// marker also makes a restart or takeover reset yesterday's totals correctly.
func (e *Engine) resetDailyStats(now time.Time) {
	local := now.In(time.Local)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.Local).Unix()
	e.mu.Lock()
	defer e.mu.Unlock()
	previous := e.state.Stats["day_epoch"]
	if previous != 0 && int64(previous) != day {
		for key := range e.state.Stats {
			if key != "started" {
				delete(e.state.Stats, key)
			}
		}
	}
	e.state.Stats["day_epoch"] = float64(day)
}
