package engine

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/soulteary/owlcron/types"
)

// Recovery never sends a signal using an unverified persisted PID. A kernel
// start identity protects against reuse; the launch token also finds descendants
// when their group leader exited, or when exec preceded the PID metadata write.
func stopRecoveredJobProcess(job *types.Job) error {
	type proof struct {
		pid      int
		identity string
	}
	groups := make(map[int][]proof)
	if job.PID > 1 {
		identity, err := processIdentity(job.PID)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cannot inspect interrupted process identity")
		}
		if err == nil && job.ProcessIdentity != "" && identity != job.ProcessIdentity {
			return nil
		}
		if err == nil && identity == job.ProcessIdentity && identity != "" {
			group, err := syscall.Getpgid(job.PID)
			if err != nil && !errors.Is(err, syscall.ESRCH) {
				return fmt.Errorf("cannot inspect interrupted process group")
			}
			if err == nil {
				if group != job.PID {
					return fmt.Errorf("interrupted process group ownership changed")
				}
				groups[group] = append(groups[group], proof{job.PID, identity})
			}
		}
	}
	if job.ProcessToken != "" && len(groups) == 0 {
		group := job.PID
		if group < 2 {
			group = 0
		}
		members, err := processGroupMembers(group)
		if err != nil {
			return fmt.Errorf("cannot enumerate interrupted process group")
		}
		for _, pid := range members {
			identityBefore, err := processIdentity(pid)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				if group == 0 {
					continue
				}
				return fmt.Errorf("cannot inspect interrupted process identity")
			}
			owned, err := processHasRecoveryToken(pid, job.ProcessToken)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				if group == 0 {
					continue
				}
				return fmt.Errorf("cannot verify interrupted process ownership")
			}
			if !owned {
				continue
			}
			pgid, err := syscall.Getpgid(pid)
			if errors.Is(err, syscall.ESRCH) {
				continue
			}
			if err != nil || pgid <= 1 || pgid == syscall.Getpgrp() {
				return fmt.Errorf("unsafe interrupted process group")
			}
			identity, err := processIdentity(pid)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return fmt.Errorf("cannot recheck interrupted process identity")
			}
			if identity != identityBefore {
				continue
			}
			groups[pgid] = append(groups[pgid], proof{pid, identity})
		}
	} else if job.PID > 1 && len(groups) == 0 {
		if _, err := processIdentity(job.PID); err == nil {
			return fmt.Errorf("live legacy process requires manual identity verification")
		}
	}
	for group, proofs := range groups {
		if group <= 1 || group == syscall.Getpgrp() {
			return fmt.Errorf("refusing to signal daemon process group")
		}
		// Recheck the same kernel identity immediately before signalling. Never
		// turn a stale group number into permission to signal unrelated processes.
		verified := false
		for _, p := range proofs {
			identity, err := processIdentity(p.pid)
			if err == nil && identity == p.identity {
				pgid, err := syscall.Getpgid(p.pid)
				if err == nil && pgid == group {
					verified = true
					break
				}
			}
		}
		if !verified {
			members, err := processGroupMembers(group)
			if err != nil {
				return fmt.Errorf("cannot recheck interrupted process group")
			}
			if len(members) == 0 {
				delete(groups, group)
				continue
			}
			if job.ProcessToken != "" {
				for _, pid := range members {
					before, err := processIdentity(pid)
					if err != nil {
						continue
					}
					owned, err := processHasRecoveryToken(pid, job.ProcessToken)
					if err != nil || !owned {
						continue
					}
					pgid, err := syscall.Getpgid(pid)
					if err != nil || pgid != group {
						continue
					}
					after, err := processIdentity(pid)
					if err == nil && before == after {
						verified = true
						break
					}
				}
			}
			if !verified {
				return fmt.Errorf("interrupted process ownership changed before cleanup")
			}
		}

		if err := syscall.Kill(-group, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("cannot stop interrupted process group")
		}
	}
	if len(groups) == 0 || job.ProcessToken == "" {
		return nil
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		live := false
		for group := range groups {
			members, err := processGroupMembers(group)
			if err != nil {
				return fmt.Errorf("cannot confirm interrupted process cleanup")
			}
			// Ownership was proved before signalling. Confirm group quiescence
			// using kernel liveness, not environment reads from dying processes.
			live = live || len(members) > 0
		}
		if !live {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("interrupted process cleanup did not finish")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
