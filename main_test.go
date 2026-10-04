package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsInvalidOptions(t *testing.T) {
	for _, flag := range []string{"conns", "bytes", "timeout"} {
		for _, value := range []int{-1, 0} {
			opt := options{conns: 1, bytes: 32, timeout: time.Second}
			switch flag {
			case "conns":
				opt.conns = value
			case "bytes":
				opt.bytes = int64(value)
			case "timeout":
				opt.timeout = time.Duration(value)
			}
			err := run(context.Background(), opt)
			if err == nil || !strings.Contains(err.Error(), "-"+flag) {
				t.Fatalf("%s=%d: expected validation error, got %v", flag, value, err)
			}
		}
	}
}

func TestReportJSONLines(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "output")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	stdout := os.Stdout
	os.Stdout = file
	defer func() { os.Stdout = stdout }()
	for range 2 {
		if err := report(options{json: true, upload: true}, Client{}, Result{InstBytesPS: 1_000_000}, Result{InstBytesPS: 500_000}, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	for _, line := range lines {
		var out jsonOutput
		if err := json.Unmarshal([]byte(line), &out); err != nil {
			t.Fatal(err)
		}
		if out.DownloadMbps != 8 || out.UploadMbps != 4 || out.LatencyMs != 1 {
			t.Fatalf("unexpected output: %+v", out)
		}
	}
}
