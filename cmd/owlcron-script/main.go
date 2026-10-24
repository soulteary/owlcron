// owlcron-script adapts a fixed administrator-configured command to OwlCron's
// newline-delimited plugin protocol. Script output is always treated as logs.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	command := os.Args[1:]
	if len(command) > 0 && command[0] == "--" {
		command = command[1:]
	}
	os.Exit(run(ctx, command, os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, command []string, input io.Reader, output, logs io.Writer) int {
	code, err := execute(ctx, command, input, logs)
	result := struct {
		Complete    bool   `json:"complete"`
		Code        int    `json:"code"`
		Description string `json:"description,omitempty"`
	}{Complete: true, Code: code}
	if err != nil {
		result.Description = err.Error()
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return 1
	}
	return code
}

func execute(ctx context.Context, command []string, input io.Reader, logs io.Writer) (int, error) {
	if len(command) == 0 || !filepath.IsAbs(command[0]) {
		return 1, errors.New("an absolute administrator-configured command is required")
	}
	reader := bufio.NewScanner(input)
	reader.Buffer(make([]byte, 4096), 1024*1024)
	if !reader.Scan() {
		return 1, errors.New("missing or oversized job input")
	}
	raw := append([]byte(nil), reader.Bytes()...)
	var job struct {
		Params map[string]json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &job); err != nil {
		return 1, errors.New("invalid job JSON")
	}
	// Event input supplies arguments only. It can never select a command or cwd.
	var args []string
	if value, ok := job.Params["args"]; ok {
		var encoded string
		if json.Unmarshal(value, &encoded) != nil {
			return 1, errors.New("args must be a JSON-array string")
		}
		if len(encoded) > 65536 || json.Unmarshal([]byte(encoded), &args) != nil || len(args) > 256 {
			return 1, errors.New("args must encode at most 256 strings within 64 KiB")
		}
	}
	cmd := exec.CommandContext(ctx, command[0], append(append([]string(nil), command[1:]...), args...)...)
	cmd.Stdin = bytes.NewReader(append(raw, '\n'))
	// Both streams bypass protocol parsing. Only this adapter reports completion.
	cmd.Stdout = logs
	cmd.Stderr = logs
	cmd.WaitDelay = time.Second
	err := cmd.Run()
	if err == nil {
		return 0, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() > 0 {
		return exit.ExitCode(), fmt.Errorf("script exited with code %d", exit.ExitCode())
	}
	if ctx.Err() != nil {
		return 1, errors.New("script cancelled")
	}
	return 1, errors.New("script could not complete")
}
