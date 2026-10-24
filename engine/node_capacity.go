package engine

import (
	"fmt"
	"github.com/soulteary/owlcron/types"
	"regexp"
)

// Matching groups constrain a host even when a job targets it directly. Calls
// run under the admission mutex: activeJobs includes reservations, not just PIDs.
func (jm *JobManager) nodeCapacity() (map[string]int, map[string]int, error) {
	groups, err := jm.engine.LoadServerGroupsFromStorage()
	if err != nil {
		return nil, nil, err
	}
	counts := map[string]int{}
	for _, job := range jm.engine.GetActiveJobs() {
		counts[jm.capacityHostname(job.Hostname)]++
	}
	limits := map[string]int{}
	hosts := jm.engine.comm.GetAllServers()
	hosts[jm.getHostname()] = nil
	for host := range hosts {
		for _, group := range groups {
			if group.MaxJobs <= 0 || group.Regexp == "" {
				continue
			}
			re, err := regexp.Compile(group.Regexp)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid capacity group: %w", err)
			}
			if re.MatchString(host) && (limits[host] == 0 || group.MaxJobs < limits[host]) {
				limits[host] = group.MaxJobs
			}
		}
	}
	return counts, limits, nil
}
func (jm *JobManager) capacityHostname(host string) string {
	if host == "local" || host == "" {
		return jm.getHostname()
	}
	return host
}
func (jm *JobManager) availableServers(hosts []string) []string {
	counts, limits, err := jm.nodeCapacity()
	if err != nil {
		return nil
	}
	available := make([]string, 0, len(hosts))
	for _, host := range hosts {
		name := jm.capacityHostname(host)
		if limits[name] == 0 || counts[name] < limits[name] {
			available = append(available, host)
		}
	}
	return available
}
func (jm *JobManager) checkNodeCapacity(jobs []*types.Job) error {
	counts, limits, err := jm.nodeCapacity()
	if err != nil {
		return fmt.Errorf("load node capacity: %w", err)
	}
	for _, job := range jobs {
		host := jm.capacityHostname(job.Hostname)
		counts[host]++
		if limits[host] > 0 && counts[host] > limits[host] {
			return fmt.Errorf("node %s has reached its maximum of %d jobs", host, limits[host])
		}
	}
	return nil
}
