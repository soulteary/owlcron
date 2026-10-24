package engine

import (
	"fmt"
	"github.com/soulteary/owlcron/types"
	"regexp"
	"testing"
)

func TestCapacityLimitsSnapshot(t *testing.T) {
	groups := map[string]*types.ServerGroup{
		"wide":     {Regexp: "^worker-", MaxJobs: 5},
		"strict":   {Regexp: "^worker-1$", MaxJobs: 2},
		"disabled": {Regexp: "[", MaxJobs: 0},
	}
	limits, err := capacityLimits([]string{"worker-1", "worker-2", "other"}, groups)
	if err != nil || limits["worker-1"] != 2 || limits["worker-2"] != 5 || limits["other"] != 0 {
		t.Fatalf("limits=%v error=%v", limits, err)
	}
	groups["strict"].MaxJobs = 1
	limits, err = capacityLimits([]string{"worker-1"}, groups)
	if err != nil || limits["worker-1"] != 1 {
		t.Fatalf("new snapshot not applied: %v %v", limits, err)
	}
	groups["invalid"] = &types.ServerGroup{Regexp: "[", MaxJobs: 1}
	if _, err = capacityLimits([]string{"worker-1"}, groups); err == nil {
		t.Fatal("active invalid expression accepted")
	}
}

func BenchmarkCapacityMatching(b *testing.B) {
	hosts := make([]string, 100)
	groups := make(map[string]*types.ServerGroup)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("worker-%d", i)
	}
	for i := 0; i < 20; i++ {
		groups[fmt.Sprint(i)] = &types.ServerGroup{Regexp: fmt.Sprintf("^worker-[%d]", i%10), MaxJobs: i + 1}
	}
	b.Run("CompilePerHost", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			limits := make(map[string]int)
			for _, host := range hosts {
				for _, group := range groups {
					re, err := regexp.Compile(group.Regexp)
					if err != nil {
						b.Fatal(err)
					}
					if re.MatchString(host) && (limits[host] == 0 || group.MaxJobs < limits[host]) {
						limits[host] = group.MaxJobs
					}
				}
			}
		}
	})
	b.Run("CompilePerGroup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := capacityLimits(hosts, groups); err != nil {
				b.Fatal(err)
			}
		}
	})
}
