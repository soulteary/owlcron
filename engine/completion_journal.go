package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/soulteary/owlcron/types"
)

const (
	completionJournalHeader  = 64
	completionTerminalMargin = 4096
	maxCompletionResultBytes = 256 * 1024
	completionPhaseActive    = "active"
	completionPhaseReported  = "reported"
	completionPhaseSealed    = "sealed"
)

var (
	completionJournalMagic      = [8]byte{'O', 'W', 'L', 'C', 'O', 'M', 'P', 1}
	errCompletionCheckpoint     = errors.New("completion recovery checkpoint unavailable")
	ErrCompletionResultTooLarge = errors.New("plugin result exceeds 256 KiB recovery limit")
)

type completionCheckpoint struct {
	Phase      string     `json:"phase"`
	Job        *types.Job `json:"job"`
	RecordedAt float64    `json:"recorded_at,omitempty"`
}

type completionJournal struct {
	checkpoint completionCheckpoint
	generation uint64
	slot       int
	capacity   int64
}

func (jm *JobManager) completionJournalPath(id string) string {
	return filepath.Join(jm.engine.config.LogDir, "jobs", id+".completion-wal")
}

func checkpointBytes(job *types.Job, phase string) ([]byte, error) {
	snapshot := types.CloneJob(job)
	snapshot.StorageError = ""
	sanitizeJobReports(snapshot)
	checkpoint := completionCheckpoint{Phase: phase, Job: snapshot}
	if phase == completionPhaseReported {
		checkpoint.RecordedAt = float64(time.Now().UnixNano()) / 1e9
	}
	return json.Marshal(checkpoint)
}

// The complete mutable plugin envelope is bounded before accepting any update.
// Its entire capacity is reserved before exec, so an otherwise valid report
// never needs to acquire more spool space after logs or queues have filled it.
func completionReservationSize(job *types.Job) (int64, error) {
	base := types.CloneJob(job)
	sanitizeJobReports(base)
	result := map[string]interface{}{
		"description": base.Description, "perf": base.Perf, "table": base.Table,
		"html": base.HTML, "label": base.Label, "update_event": base.UpdateEvent,
		"chain": base.Chain, "chain_error": base.ChainError,
		"chain_data": base.ChainData, "chain_params": base.ChainParams,
		"notify_success": base.NotifySuccess, "notify_fail": base.NotifyFail,
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return 0, err
	}
	if len(raw) > maxCompletionResultBytes {
		return 0, ErrCompletionResultTooLarge
	}
	base.Description = ""
	base.Perf, base.Table, base.HTML = nil, nil, nil
	base.Label = ""
	base.UpdateEvent = nil
	base.Chain, base.ChainError = "", ""
	base.ChainData, base.ChainParams = nil, nil
	base.NotifySuccess, base.NotifyFail = "", ""
	raw, err = checkpointBytes(base, completionPhaseActive)
	if err != nil {
		return 0, err
	}
	return int64(len(raw)) + maxCompletionResultBytes + completionTerminalMargin, nil
}

func completionHeader(generation uint64, raw []byte) []byte {
	header := make([]byte, completionJournalHeader)
	copy(header, completionJournalMagic[:])
	binary.BigEndian.PutUint64(header[8:16], generation)
	binary.BigEndian.PutUint64(header[16:24], uint64(len(raw)))
	hash := sha256.New()
	_, _ = hash.Write(header[:24])
	_, _ = hash.Write(raw)
	copy(header[24:56], hash.Sum(nil))
	return header
}

// Two checked slots preserve the previous durable checkpoint while the inactive
// slot is being written. Every payload byte is physically allocated before exec;
// a final checkpoint never needs another temporary file or additional quota.
func readCompletionJournal(path string) (*completionJournal, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size()%2 != 0 || info.Size() < 2*(completionJournalHeader+1) {
		return nil, fmt.Errorf("invalid completion journal layout")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	slotSize := info.Size() / 2
	var newest *completionJournal
	for slot := range 2 {
		header := make([]byte, completionJournalHeader)
		if _, err := file.ReadAt(header, int64(slot)*slotSize); err != nil || string(header[:8]) != string(completionJournalMagic[:]) {
			continue
		}
		generation := binary.BigEndian.Uint64(header[8:16])
		length := binary.BigEndian.Uint64(header[16:24])
		if generation == 0 || length == 0 || length > uint64(slotSize-completionJournalHeader) || length > uint64(math.MaxInt) {
			continue
		}
		raw := make([]byte, int(length))
		if _, err := file.ReadAt(raw, int64(slot)*slotSize+completionJournalHeader); err != nil {
			continue
		}
		if expected := completionHeader(generation, raw); string(header[24:56]) != string(expected[24:56]) {
			continue
		}
		var checkpoint completionCheckpoint
		if json.Unmarshal(raw, &checkpoint) != nil || checkpoint.Job == nil || checkpoint.Job.ID == "" {
			continue
		}
		if checkpoint.Phase != completionPhaseActive && checkpoint.Phase != completionPhaseReported && checkpoint.Phase != completionPhaseSealed {
			continue
		}
		if newest == nil || generation > newest.generation {
			newest = &completionJournal{checkpoint: checkpoint, generation: generation, slot: slot, capacity: slotSize - completionJournalHeader}
		}
	}
	if newest == nil {
		return nil, fmt.Errorf("completion journal has no valid checkpoint")
	}
	return newest, nil
}

// ensureCompletionReservation is called under finishMu before accepting a local
// snapshot. Growth is atomic and accounts for both old and replacement files.
// Remote snapshots are reserved by their executing worker instead.
func (jm *JobManager) ensureCompletionReservation(job *types.Job) error {
	if job.Hostname != "local" && job.Hostname != jm.getHostname() {
		return nil
	}
	required, err := completionReservationSize(job)
	if err != nil {
		return err
	}
	raw, err := checkpointBytes(job, completionPhaseActive)
	if err != nil {
		return err
	}
	path := jm.completionJournalPath(job.ID)
	current, err := readCompletionJournal(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if current != nil && required <= current.capacity {
		return nil
	}
	capacity := required
	if capacity > math.MaxInt64-4095 {
		return ErrSpoolCapacityExceeded
	}
	capacity = (capacity + 4095) / 4096 * 4096
	if capacity > int64(math.MaxInt)/2-completionJournalHeader {
		return ErrSpoolCapacityExceeded
	}
	// Preserve the latest checked state during growth. The candidate is only
	// accepted after this allocation succeeds and its checkpoint is committed.
	generation := uint64(1)
	if current != nil {
		raw, err = json.Marshal(current.checkpoint)
		if err != nil {
			return err
		}
		if current.generation == math.MaxUint64 {
			return fmt.Errorf("completion checkpoint generation exhausted")
		}
		generation = current.generation + 1
	}
	allocation := 2 * (capacity + completionJournalHeader)
	// Check before allocating the physical reservation buffer: a rejected
	// oversized API snapshot must not allocate beyond the configured quota.
	// writeSpoolFile rechecks while holding the budget through the actual write.
	if err := jm.engine.checkSpoolWriteSize(allocation); err != nil {
		return err
	}
	data := make([]byte, int(allocation))
	copy(data, completionHeader(generation, raw))
	copy(data[completionJournalHeader:], raw)
	return jm.engine.writeSpoolFile(path, data, ".completion-reserve-*")
}

// completionReservationFits checks terminal snapshots without allocating more
// bytes, including when the spool is full and an abort must still be recorded.
func (jm *JobManager) completionReservationFits(job *types.Job) error {
	if job.Hostname != "local" && job.Hostname != jm.getHostname() {
		return nil
	}
	current, err := readCompletionJournal(jm.completionJournalPath(job.ID))
	if err != nil {
		return err
	}
	raw, err := checkpointBytes(job, completionPhaseSealed)
	if err != nil {
		return err
	}
	if int64(len(raw)) > current.capacity {
		return ErrSpoolCapacityExceeded
	}
	return nil
}

// writeCompletionCheckpoint uses only the already allocated inactive slot. A
// crash during payload/header writes leaves the previous checksum valid.
func (jm *JobManager) writeCompletionCheckpoint(job *types.Job, phase string) error {
	path := jm.completionJournalPath(job.ID)
	current, err := readCompletionJournal(path)
	if err != nil {
		return err
	}
	raw, err := checkpointBytes(job, phase)
	if err != nil {
		return err
	}
	if int64(len(raw)) > current.capacity {
		return ErrSpoolCapacityExceeded
	}
	if current.generation == math.MaxUint64 {
		return fmt.Errorf("completion checkpoint generation exhausted")
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	offset := int64(1-current.slot) * (current.capacity + completionJournalHeader)
	if n, err := file.WriteAt(raw, offset+completionJournalHeader); err != nil {
		return err
	} else if n != len(raw) {
		return io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return err
	}
	header := completionHeader(current.generation+1, raw)
	if n, err := file.WriteAt(header, offset); err != nil {
		return err
	} else if n != len(header) {
		return io.ErrShortWrite
	}
	return file.Sync()
}

func boundedCompletionDiagnostic(message string) string {
	if len(message) <= 256 {
		return message
	}
	message = message[:256]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

func (jm *JobManager) removeCompletionSpool(id string) error {
	var result error
	for _, path := range []string{jm.completionJournalPath(id), jm.legacyCompletionSpoolPath(id)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			result = errors.Join(result, err)
		}
	}
	if result != nil {
		return result
	}
	dir, err := os.Open(filepath.Join(jm.engine.config.LogDir, "jobs"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return err
	}
	if jm.engine.spoolBudget != nil {
		return jm.engine.spoolBudget.refresh(jm.engine.ctx)
	}
	return nil
}

// An unsealed report is recoverable only after reclaiming its verified process
// group. Live maintenance skips it while the execution goroutine owns the job.
func (jm *JobManager) recoverCompletionJournal(path string, startup bool) (*types.Job, error) {
	journal, err := readCompletionJournal(path)
	if err != nil {
		return nil, err
	}
	job := journal.checkpoint.Job
	if jm.completionJournalPath(job.ID) != path {
		return nil, fmt.Errorf("invalid completion journal job")
	}
	jm.mu.RLock()
	_, running := jm.running[job.ID]
	jm.mu.RUnlock()
	if running || journal.checkpoint.Phase == completionPhaseActive || journal.checkpoint.Phase != completionPhaseSealed && !startup {
		return nil, nil
	}
	if !job.Complete {
		return nil, fmt.Errorf("invalid completion journal result")
	}
	if journal.checkpoint.Phase != completionPhaseSealed {
		if err := stopRecoveredJobProcess(job); err != nil {
			return nil, err
		}
		if job.AbortReason != "" {
			job.Code = 1
			job.AbortReason = boundedCompletionDiagnostic(job.AbortReason)
			job.Description = "Job Aborted: " + job.AbortReason
		}
		if job.TimeEnd <= 0 {
			job.TimeEnd = journal.checkpoint.RecordedAt
			if job.TimeEnd <= 0 {
				return nil, fmt.Errorf("completion report has no durable timestamp")
			}
			job.Elapsed = job.TimeEnd - job.TimeStart
		}
		job.Action = "job_complete"
		job.Pending = false
		if job.Code == 0 {
			job.Progress = 1
		}
		if job.LogFile != "" {
			file, err := os.OpenFile(job.LogFile, os.O_RDWR, 0)
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			}
			if err == nil {
				info, statErr := file.Stat()
				syncErr := file.Sync()
				_ = file.Close()
				if statErr != nil || syncErr != nil {
					return nil, errors.Join(statErr, syncErr)
				}
				job.LogFileSize = info.Size()
			}
		}
		if err := jm.writeCompletionCheckpoint(job, completionPhaseSealed); err != nil {
			return nil, err
		}
	}
	return job, nil
}

func isCompletionJournal(path string) bool { return strings.HasSuffix(path, ".completion-wal") }
