package engine

import (
	"regexp"
	"sort"
	"time"

	"github.com/soulteary/owlcron/types"
)

// chooseServers 选择服务器
func (jm *JobManager) chooseServers(event *types.Event) []string {
	// 如果目标是 "local" 或空，返回本地服务器
	if event.Target == "" || event.Target == "local" {
		return jm.availableServers([]string{"local"})
	}

	// 从存储加载服务器组
	var servers []string
	allServers := jm.engine.GetCommunication().GetAllServers()

	// 如果指定了服务器组，从存储加载并用正则过滤
	if event.Target != "" && event.Target != "local" {
		groups, err := jm.engine.LoadServerGroupsFromStorage()
		if err == nil {
			if group, exists := groups[event.Target]; exists {
				if group.Regexp != "" {
					re, reErr := regexp.Compile(group.Regexp)
					if reErr == nil {
						for hostname := range allServers {
							if !allServers[hostname].Disabled && re.MatchString(hostname) {
								servers = append(servers, hostname)
							}
						}
					}
				}

			}
		}
	}

	if len(servers) == 0 {
		if server, ok := allServers[event.Target]; ok && !server.Disabled {
			servers = []string{event.Target}
		}
	}
	if len(servers) == 0 {
		return nil
	}
	servers = jm.availableServers(servers)
	if len(servers) == 0 {
		return nil
	}
	sort.Strings(servers)

	// Multiplex: return all servers (one job per server)
	if event.Multiplex.Bool() {
		return servers
	}

	switch event.Algo {
	case "random":
		return []string{jm.chooseRandomServer(servers)}
	case "round_robin":
		jm.engine.mu.Lock()
		robin := jm.engine.state.Robins[event.ID]
		jm.engine.state.Robins[event.ID] = (robin + 1) % len(servers)
		selected := servers[robin%len(servers)]
		jm.engine.mu.Unlock()
		return []string{selected}
	case "least_busy":
		return []string{jm.chooseLeastBusyServer(servers, allServers)}
	case "least_cpu":
		return []string{jm.chooseLeastCPUServer(servers, allServers)}
	case "least_mem":
		return []string{jm.chooseLeastMemServer(servers, allServers)}
	case "prefer_first":
		sort.Strings(servers)
		return []string{servers[0]}
	case "prefer_last":
		sort.Strings(servers)
		return []string{servers[len(servers)-1]}
	default:
		if len(servers) == 1 {
			return servers
		}
		return []string{jm.chooseRandomServer(servers)}
	}
}

// chooseRandomServer 随机选择服务器
func (jm *JobManager) chooseRandomServer(servers []string) string {
	if len(servers) == 0 {
		return "local"
	}
	if len(servers) == 1 {
		return servers[0]
	}
	// 使用时间戳作为随机种子
	index := int(time.Now().UnixNano()) % len(servers)
	return servers[index]
}

// chooseLeastBusyServer 选择最不忙的服务器
func (jm *JobManager) chooseLeastBusyServer(servers []string, allServers map[string]*types.Server) string {
	if len(servers) == 0 {
		return "local"
	}

	// 计算每个服务器的忙碌程度（活动任务数）
	activeJobs := jm.engine.GetActiveJobs()
	serverBusyCount := make(map[string]int)

	for _, job := range activeJobs {
		if count, exists := serverBusyCount[job.Hostname]; exists {
			serverBusyCount[job.Hostname] = count + 1
		} else {
			serverBusyCount[job.Hostname] = 1
		}
	}

	// 找到最不忙的服务器
	leastBusy := servers[0]
	leastBusyCount := serverBusyCount[leastBusy]

	for _, server := range servers[1:] {
		count := serverBusyCount[server]
		if count < leastBusyCount {
			leastBusy = server
			leastBusyCount = count
		}
	}

	return leastBusy
}

// chooseLeastCPUServer 选择 CPU 使用最低的服务器
func (jm *JobManager) chooseLeastCPUServer(servers []string, allServers map[string]*types.Server) string {
	if len(servers) == 0 {
		return "local"
	}

	best := servers[0]
	bestCPU := float64(999999)

	for _, server := range servers {
		s, ok := allServers[server]
		if !ok {
			continue
		}
		cpu := float64(0)
		if s.Data != nil {
			if v, ok := s.Data["cpu"].(float64); ok {
				cpu = v
			}
		}
		if cpu < bestCPU {
			bestCPU = cpu
			best = server
		}
	}

	return best
}

// chooseLeastMemServer 选择内存使用最低的服务器
func (jm *JobManager) chooseLeastMemServer(servers []string, allServers map[string]*types.Server) string {
	if len(servers) == 0 {
		return "local"
	}

	best := servers[0]
	bestMem := float64(999999999999)

	for _, server := range servers {
		s, ok := allServers[server]
		if !ok {
			continue
		}
		mem := float64(0)
		if s.Data != nil {
			if v, ok := s.Data["mem"].(float64); ok {
				mem = v
			}
		}
		if mem < bestMem {
			bestMem = mem
			best = server
		}
	}

	return best
}
