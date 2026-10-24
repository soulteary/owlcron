package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/soulteary/owlcron/types"
	"go.uber.org/zap"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type PluginExecutor struct{ logger *zap.Logger }

func NewPluginExecutor(logger *zap.Logger) *PluginExecutor { return &PluginExecutor{logger: logger} }

// ExecutePrepared runs the Cronicle newline JSON protocol and drains all output,
// including output written after completion, before reaping the child.
func (pe *PluginExecutor) ExecutePrepared(ctx context.Context, cmd *exec.Cmd, job *types.Job, log io.Writer, started func(int), update func(map[string]interface{})) (*PluginResult, error) {
	input, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	cmd.Stdin = bytes.NewReader(append(input, '\n'))
	result := &PluginResult{}
	safeLog := &lockedWriter{writer: log, onError: func() {
		// A failed pipe copy does not make exec.Wait kill a still-running child.
		// Use the caller's process-group cancellation contract immediately.
		if cmd.Cancel != nil {
			_ = cmd.Cancel()
		} else if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}}
	output := &protocolWriter{log: safeLog, result: result, update: update}
	cmd.Stdout = output
	cmd.Stderr = safeLog
	output.mu.Lock()
	if err = cmd.Start(); err != nil {
		output.mu.Unlock()
		return result, fmt.Errorf("failed to start plugin: %w", err)
	}
	if started != nil {
		started(cmd.Process.Pid)
	}
	output.mu.Unlock()
	err = cmd.Wait()
	output.flush()
	if logErr := safeLog.Err(); logErr != nil {
		return result, logErr
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if output.err != nil {
		return result, output.err
	}
	if !result.Complete {
		if err != nil {
			return result, fmt.Errorf("plugin exited without reporting completion: %w", err)
		}
		return result, fmt.Errorf("plugin exited without reporting completion")
	}
	// A complete message is authoritative, matching Cronicle's plugin contract.
	return result, nil
}

func (pe *PluginExecutor) Execute(ctx context.Context, job *types.Job, plug *types.Plugin) (*PluginResult, error) {
	command, args := ParseCommand(plug.Command)
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = plug.CWD
	cmd.Env = os.Environ()
	for k, v := range plug.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return pe.ExecutePrepared(ctx, cmd, job, io.Discard, func(pid int) { job.PID = pid }, nil)
}

type lockedWriter struct {
	mu      sync.Mutex
	writer  io.Writer
	err     error
	onError func()
}

func (w *lockedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, err
	}
	n, err := w.writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.err = err
	callback := w.onError
	w.mu.Unlock()
	if err != nil && callback != nil {
		callback()
	}
	return n, err
}
func (w *lockedWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

type protocolWriter struct {
	mu     sync.Mutex
	buf    []byte
	log    io.Writer
	result *PluginResult
	update func(map[string]interface{})
	err    error
}

func (w *protocolWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	w.buf = append(w.buf, p...)
	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.line(w.buf[:idx])
		w.buf = w.buf[idx+1:]
	}
	if len(w.buf) > 4*1024*1024 {
		w.writeLog(w.buf)
		w.buf = nil
	}
	return n, w.err
}
func (w *protocolWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.line(w.buf)
		w.buf = nil
	}
}
func (w *protocolWriter) line(line []byte) {
	var data map[string]interface{}
	if json.Unmarshal(line, &data) != nil || !isProtocolMessage(data) {
		w.writeLog(append(append([]byte(nil), line...), '\n'))
		return
	}
	if w.result.Complete {
		return
	}
	if p, ok := data["progress"].(float64); ok {
		if p < 0 {
			p = 0
		}
		if p > 1 {
			p = 1
		}
		w.result.Progress = p
		data["progress"] = p
	}
	if c, ok := data["complete"]; ok {
		switch x := c.(type) {
		case bool:
			w.result.Complete = x
		case float64:
			w.result.Complete = x != 0
		}
		data["complete"] = w.result.Complete
	}
	if c, ok := data["code"].(float64); ok {
		w.result.Code = int(c)
	}
	if d, ok := data["description"].(string); ok {
		w.result.Description = d
	}
	if p, ok := data["perf"]; ok {
		w.result.Perf = p
	}
	if c, ok := data["chain"].(string); ok {
		w.result.Chain = c
	}
	if c, ok := data["chain_error"].(string); ok {
		w.result.ChainError = c
	}
	if c, ok := data["chain_data"].(map[string]interface{}); ok {
		w.result.ChainData = c
	}
	if c, ok := data["chain_params"].(map[string]interface{}); ok {
		w.result.ChainParams = c
	}
	if w.update != nil {
		w.update(data)
	}
}

func (w *protocolWriter) writeLog(data []byte) {
	n, err := w.log.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if w.err == nil {
		w.err = err
	}
}
func isProtocolMessage(data map[string]interface{}) bool {
	for _, k := range []string{"progress", "complete", "code", "description", "perf", "chain", "chain_error", "chain_data", "chain_params", "notify_success", "notify_fail", "table", "html", "label", "update_event"} {
		if _, ok := data[k]; ok {
			return true
		}
	}
	return false
}

// ParseCommand splits an executable command without invoking a shell. Shell syntax
// is supported by explicitly choosing /bin/sh -c in the plugin command.
func ParseCommand(command string) (string, []string) {
	var parts []string
	var current strings.Builder
	quote := byte(0)
	started := false
	for i := 0; i < len(command); i++ {
		ch := command[i]
		if ch == '\\' && quote != '\'' && i+1 < len(command) {
			i++
			current.WriteByte(command[i])
			started = true
			continue
		}
		if quote != 0 {
			if ch == quote {
				quote = 0
			} else {
				current.WriteByte(ch)
			}
			started = true
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			started = true
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\n' {
			if started {
				parts = append(parts, current.String())
				current.Reset()
				started = false
			}
			continue
		}
		current.WriteByte(ch)
		started = true
	}
	if started {
		parts = append(parts, current.String())
	}
	if len(parts) == 0 {
		return "", nil
	}
	return parts[0], parts[1:]
}

type PluginResult struct {
	Progress    float64                `json:"progress"`
	Complete    bool                   `json:"complete"`
	Code        int                    `json:"code"`
	Description string                 `json:"description"`
	Perf        interface{}            `json:"perf"`
	Chain       string                 `json:"chain,omitempty"`
	ChainError  string                 `json:"chain_error,omitempty"`
	ChainData   map[string]interface{} `json:"chain_data,omitempty"`
	ChainParams map[string]interface{} `json:"chain_params,omitempty"`
}

// PluginManager 插件管理器
type PluginManager struct {
	plugins map[string]*types.Plugin
	mu      sync.RWMutex
	logger  *zap.Logger
}

// NewPluginManager 创建插件管理器
func NewPluginManager(logger *zap.Logger) *PluginManager {
	return &PluginManager{
		plugins: make(map[string]*types.Plugin),
		logger:  logger,
	}
}

// AddPlugin 添加插件
func (pm *PluginManager) AddPlugin(plugin *types.Plugin) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.plugins[plugin.ID] = plugin
}

// GetPlugin 获取插件
func (pm *PluginManager) GetPlugin(id string) (*types.Plugin, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	plugin, exists := pm.plugins[id]
	return plugin, exists
}

// RemovePlugin 移除插件
func (pm *PluginManager) RemovePlugin(id string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.plugins, id)
}

// GetAllPlugins 获取所有插件
func (pm *PluginManager) GetAllPlugins() map[string]*types.Plugin {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make(map[string]*types.Plugin)
	for k, v := range pm.plugins {
		result[k] = v
	}
	return result
}
