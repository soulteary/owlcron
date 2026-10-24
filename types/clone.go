package types

// CloneJob returns a snapshot which shares no mutable maps or resource counters.
func CloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	cp := *job
	cp.Params = CloneMap(job.Params)
	cp.ChainData = CloneMap(job.ChainData)
	cp.ChainParams = CloneMap(job.ChainParams)
	cp.Env = make(map[string]string, len(job.Env))
	for k, v := range job.Env {
		cp.Env[k] = v
	}
	if job.CPU != nil {
		usage := *job.CPU
		cp.CPU = &usage
	}
	if job.Mem != nil {
		usage := *job.Mem
		cp.Mem = &usage
	}
	cp.Perf = cloneValue(job.Perf)
	cp.Table = cloneValue(job.Table)
	cp.HTML = cloneValue(job.HTML)
	cp.UpdateEvent = CloneMap(job.UpdateEvent)
	return &cp
}

func CloneEvent(event *Event) *Event {
	if event == nil {
		return nil
	}
	cp := *event
	cp.Params = CloneMap(event.Params)
	cp.ChainData = CloneMap(event.ChainData)
	cp.ChainParams = CloneMap(event.ChainParams)
	cp.Chain = append([]byte(nil), event.Chain...)
	cp.ChainError = append([]byte(nil), event.ChainError...)
	if event.Timing != nil {
		t := *event.Timing
		t.Minutes = append([]int(nil), t.Minutes...)
		t.Hours = append([]int(nil), t.Hours...)
		t.Weekdays = append([]int(nil), t.Weekdays...)
		t.Days = append([]int(nil), t.Days...)
		t.Months = append([]int(nil), t.Months...)
		t.Years = append([]int(nil), t.Years...)
		cp.Timing = &t
	}
	return &cp
}

func CloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		out[k] = cloneValue(v)
	}
	return out
}
func cloneValue(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return CloneMap(x)
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, item := range x {
			out[i] = cloneValue(item)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	default:
		return v
	}
}
