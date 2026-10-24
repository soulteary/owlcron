package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type JobInput struct {
	ID       string                 `json:"id"`
	Hostname string                 `json:"hostname"`
	Event    string                 `json:"event"`
	Now      int64                  `json:"now"`
	LogFile  string                 `json:"log_file"`
	Params   map[string]interface{} `json:"params"`
}

type ProgressMsg struct {
	Progress float64 `json:"progress"`
}

type CompleteMsg struct {
	Complete    int         `json:"complete"`
	Code        int         `json:"code"`
	Description string      `json:"description,omitempty"`
	HTML        *HTMLReport `json:"html,omitempty"`
	Perf        interface{} `json:"perf,omitempty"`
}

type HTMLReport struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

func writeJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal JSON: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

func main() {
	scanner := bufio.NewScanner(scanner_input())
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	if !scanner.Scan() {
		writeJSON(CompleteMsg{
			Complete:    1,
			Code:        1,
			Description: "Failed to read job data from stdin",
		})
		return
	}

	var job JobInput
	if err := json.Unmarshal(scanner.Bytes(), &job); err != nil {
		writeJSON(CompleteMsg{
			Complete:    1,
			Code:        1,
			Description: fmt.Sprintf("Failed to parse job JSON: %v", err),
		})
		return
	}

	fmt.Fprintf(os.Stderr, "Echo plugin started, job ID: %s\n", job.ID)

	message := "Hello from OwlCron Echo Plugin!"
	if msg, ok := job.Params["message"]; ok {
		if s, ok := msg.(string); ok && s != "" {
			message = s
		}
	}

	repeatCount := 1
	if r, ok := job.Params["repeat"]; ok {
		switch v := r.(type) {
		case float64:
			repeatCount = int(v)
		case string:
			fmt.Sscanf(v, "%d", &repeatCount)
		}
	}
	if repeatCount < 1 {
		repeatCount = 1
	}
	if repeatCount > 100 {
		repeatCount = 100
	}

	delayMs := 500
	if d, ok := job.Params["delay_ms"]; ok {
		switch v := d.(type) {
		case float64:
			delayMs = int(v)
		case string:
			fmt.Sscanf(v, "%d", &delayMs)
		}
	}
	if delayMs < 0 {
		delayMs = 0
	}
	if delayMs > 10000 {
		delayMs = 10000
	}

	start := time.Now()
	var outputLines []string

	for i := 1; i <= repeatCount; i++ {
		line := fmt.Sprintf("[%d/%d] %s", i, repeatCount, message)
		outputLines = append(outputLines, line)
		fmt.Fprintf(os.Stderr, "%s\n", line)

		progress := float64(i) / float64(repeatCount)
		writeJSON(ProgressMsg{Progress: progress})

		if i < repeatCount && delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
	}

	elapsed := time.Since(start)

	htmlContent := "<pre>" + escapeHTML(strings.Join(outputLines, "\n")) + "</pre>"

	writeJSON(CompleteMsg{
		Complete:    1,
		Code:        0,
		Description: fmt.Sprintf("Echo completed: %d message(s) in %v", repeatCount, elapsed.Round(time.Millisecond)),
		HTML: &HTMLReport{
			Title:   "Echo Output",
			Content: htmlContent,
		},
		Perf: map[string]interface{}{
			"total": map[string]interface{}{
				"elapsed": elapsed.Seconds(),
			},
		},
	})
}

func scanner_input() *os.File {
	return os.Stdin
}

func escapeHTML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
