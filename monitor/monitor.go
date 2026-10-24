package monitor

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

// ResourceMonitor 资源监控器
type ResourceMonitor struct {
	logger *zap.Logger
	mu     sync.RWMutex
	flags  map[string]resourceFlags
}

// NewResourceMonitor 创建资源监控器
func NewResourceMonitor(logger *zap.Logger) *ResourceMonitor {
	return &ResourceMonitor{
		logger: logger,
		flags:  make(map[string]resourceFlags),
	}
}

// MonitorJobResources 监控任务资源使用
func (rm *ResourceMonitor) MonitorJobResources(job *types.Job) error {
	if job.PID == 0 {
		return nil
	}

	// 使用 ps 命令获取进程信息
	cmd := exec.Command("ps", "-o", "pid,ppid,%cpu,rss", "-p", strconv.Itoa(job.PID))
	output, err := cmd.Output()
	if err != nil {
		return err
	}

	// 解析输出
	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return nil
	}

	// 跳过标题行，解析数据行
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}

		pid, _ := strconv.Atoi(fields[0])
		if pid != job.PID {
			continue
		}

		cpu, _ := strconv.ParseFloat(fields[2], 64)
		memKB, _ := strconv.ParseInt(fields[3], 10, 64)
		memBytes := memKB * 1024

		// 更新任务资源使用

		// 检查子进程
		childCPU, childMem := rm.checkChildProcesses(job.PID)
		rm.updateJobResources(job, cpu+childCPU, memBytes+childMem)
		break
	}

	return nil
}

// checkChildProcesses 检查子进程
func (rm *ResourceMonitor) checkChildProcesses(ppid int) (float64, int64) {
	// 获取所有进程
	cmd := exec.Command("ps", "-eo", "pid,ppid,%cpu,rss")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0
	}

	lines := strings.Split(string(output), "\n")

	var totalCPU float64
	var totalMem int64

	// 查找所有子进程（最多100层）
	children := map[int]bool{ppid: true}
	levels := 0

	for levels < 100 && len(children) > 0 {
		nextLevel := make(map[int]bool)

		for pid := range children {
			for i := 1; i < len(lines); i++ {
				line := strings.TrimSpace(lines[i])
				if line == "" {
					continue
				}

				fields := strings.Fields(line)
				if len(fields) < 4 {
					continue
				}

				childPID, _ := strconv.Atoi(fields[0])
				parentPID, _ := strconv.Atoi(fields[1])
				cpu, _ := strconv.ParseFloat(fields[2], 64)
				memKB, _ := strconv.ParseInt(fields[3], 10, 64)

				if parentPID == pid {
					totalCPU += cpu
					totalMem += memKB * 1024
					nextLevel[childPID] = true
				}
			}
		}

		children = nextLevel
		levels++
	}

	return totalCPU, totalMem
}

// updateJobResources 更新任务资源使用
func (rm *ResourceMonitor) updateJobResources(job *types.Job, cpu float64, mem int64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 更新 CPU
	if job.CPU == nil {
		job.CPU = &types.ResourceUsage{
			Min:     cpu,
			Max:     cpu,
			Total:   cpu,
			Count:   1,
			Current: cpu,
		}
	} else {
		if cpu < job.CPU.Min {
			job.CPU.Min = cpu
		}
		if cpu > job.CPU.Max {
			job.CPU.Max = cpu
		}
		job.CPU.Total += cpu
		job.CPU.Count++
		job.CPU.Current = cpu
	}

	// 更新内存
	if job.Mem == nil {
		job.Mem = &types.ResourceUsage{
			Min:     float64(mem),
			Max:     float64(mem),
			Total:   float64(mem),
			Count:   1,
			Current: float64(mem),
		}
	} else {
		memFloat := float64(mem)
		if memFloat < job.Mem.Min {
			job.Mem.Min = memFloat
		}
		if memFloat > job.Mem.Max {
			job.Mem.Max = memFloat
		}
		job.Mem.Total += memFloat
		job.Mem.Count++
		job.Mem.Current = memFloat
	}
}

// MonitorServerResources 监控服务器资源
func (rm *ResourceMonitor) MonitorServerResources() (float64, int64, error) {
	// 获取当前进程的资源使用
	pid := os.Getpid()

	cmd := exec.Command("ps", "-o", "%cpu,rss", "-p", strconv.Itoa(pid))
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return 0, 0, nil
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 2 {
		return 0, 0, nil
	}

	cpu, _ := strconv.ParseFloat(fields[0], 64)
	memKB, _ := strconv.ParseInt(fields[1], 10, 64)
	memBytes := memKB * 1024

	return cpu, memBytes, nil
}

// CheckResourceLimits 检查资源限制
type resourceFlags struct{ memory, cpu int64 }

func (rm *ResourceMonitor) Forget(id string) { rm.mu.Lock(); delete(rm.flags, id); rm.mu.Unlock() }
func (rm *ResourceMonitor) CheckResourceLimits(job *types.Job, memoryLimit int64, memorySustain int, cpuLimit int, cpuSustain int) (bool, string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	now := time.Now().Unix()
	flags := rm.flags[job.ID]
	if memoryLimit > 0 && job.Mem != nil && job.Mem.Current > float64(memoryLimit) {
		if flags.memory == 0 {
			flags.memory = now
		}
		if memorySustain <= 0 || now-flags.memory >= int64(memorySustain) {
			rm.flags[job.ID] = flags
			return true, fmt.Sprintf("Exceeded memory limit of %d bytes", memoryLimit)
		}
	} else {
		flags.memory = 0
	}
	if cpuLimit > 0 && job.CPU != nil && job.CPU.Current > float64(cpuLimit) {
		if flags.cpu == 0 {
			flags.cpu = now
		}
		if cpuSustain <= 0 || now-flags.cpu >= int64(cpuSustain) {
			rm.flags[job.ID] = flags
			return true, fmt.Sprintf("Exceeded CPU limit of %d%%", cpuLimit)
		}
	} else {
		flags.cpu = 0
	}
	rm.flags[job.ID] = flags
	job.FlaggedTime = flags.memory
	if flags.cpu > 0 && (job.FlaggedTime == 0 || flags.cpu < job.FlaggedTime) {
		job.FlaggedTime = flags.cpu
	}
	return false, ""
}
