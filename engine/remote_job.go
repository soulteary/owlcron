package engine

import (
	"fmt"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
)

func (jm *JobManager) sendJobToRemoteServer(server string, job *types.Job) error {
	jm.engine.mu.Lock()
	jm.engine.activeJobs[job.ID] = types.CloneJob(job)
	jm.engine.mu.Unlock()
	if err := jm.persistActiveJob(job); err != nil {
		jm.engine.mu.Lock()
		delete(jm.engine.activeJobs, job.ID)
		jm.engine.mu.Unlock()
		return fmt.Errorf("failed to persist remote reservation: %w", err)
	}
	if err := jm.engine.comm.SendToSlave(server, "launch_job", job); err != nil {
		jm.engine.mu.Lock()
		delete(jm.engine.activeJobs, job.ID)
		jm.engine.mu.Unlock()
		return fmt.Errorf("failed to dispatch to %s: %w", server, err)
	}
	jm.logger.Info("Job dispatched to worker", zap.String("job_id", job.ID), zap.String("server", server))
	return nil
}
