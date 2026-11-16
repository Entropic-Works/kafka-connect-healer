package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSummaryTimingsAndSuccess(t *testing.T) {
	start := time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
	result := summarize([]testEvent{
		{Action: "run", Test: "TestIntegration", Time: start},
		{Action: "output", Output: "images_test.go:45: build-healer finished in 1.2s\n"},
		{Action: "run", Test: "TestIntegration/API", Time: start.Add(3 * time.Second)},
		{Action: "pass", Test: "TestIntegration/API", Elapsed: 2},
		{Action: "pass", Test: "TestIntegration/RequestFailures", Elapsed: 8},
		{Action: "pass", Test: "TestIntegration", Elapsed: 13},
		{Action: "pass", Elapsed: 14},
	})
	if !result.passed || len(result.failures) != 0 ||
		!strings.Contains(result.markdown, "Suite setup: **3.00s**") ||
		!strings.Contains(result.markdown, "| build-healer | 1.2s |") ||
		strings.Index(result.markdown, "| RequestFailures |") > strings.Index(result.markdown, "| API |") {
		t.Fatalf("unexpected report: %+v", result)
	}
}

func TestSummaryReportsFailingLeafAndIncompleteRuns(t *testing.T) {
	result := summarize([]testEvent{
		{Action: "fail", Test: "TestIntegration/API/auth"},
		{Action: "fail", Test: "TestIntegration/API"},
		{Action: "fail", Test: "TestIntegration"},
		{Action: "fail", Elapsed: 3},
	})
	if result.passed || !slices.Equal(result.failures, []string{"TestIntegration/API/auth"}) {
		t.Fatalf("failed tests were not reported correctly: %+v", result)
	}
	result = summarize([]testEvent{{Action: "pass", Test: "TestIntegration/API", Elapsed: 1}})
	if result.passed || !strings.Contains(result.markdown, "Result: **Incomplete**") {
		t.Fatalf("incomplete run reported success: %+v", result)
	}
}

func TestReportReadsPartialJSONAndWritesSummary(t *testing.T) {
	directory := t.TempDir()
	results := filepath.Join(directory, "results.jsonl")
	if err := os.WriteFile(results, []byte("{\"Action\":\"pass\",\"Elapsed\":1}\n{\"Action\":"), 0o600); err != nil {
		t.Fatal(err)
	}
	summary := filepath.Join(directory, "summary.md")
	var stdout, stderr bytes.Buffer
	if code := run([]string{results}, &stdout, &stderr, summary, true); code != 0 {
		t.Fatalf("run = %d: %s", code, stderr.String())
	}
	data, err := os.ReadFile(summary)
	if err != nil || string(data) != stdout.String() {
		t.Fatalf("summary does not match output: %s, %v", data, err)
	}
	if err := os.WriteFile(results, []byte("{\"Action\":\"fail\",\"Test\":\"TestIntegration/API/auth\"}\n{\"Action\":\"fail\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	if code := run([]string{results}, &stdout, &stderr, "", true); code != 1 ||
		!strings.Contains(stderr.String(), "::error title=CI_INTEGRATION_FAILED::TestIntegration/API/auth") {
		t.Fatalf("missing failed-test annotation: exit %d, %s", code, stderr.String())
	}
	if code := run([]string{filepath.Join(directory, "missing.jsonl")}, &stdout, &stderr, "", false); code != 1 {
		t.Fatalf("missing results returned exit %d", code)
	}
}
