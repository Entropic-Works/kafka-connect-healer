// Command integration-report summarizes go test -json output for CI and local runs.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

type testEvent struct {
	Time    time.Time
	Action  string
	Test    string
	Elapsed float64
	Output  string
}

type report struct {
	markdown string
	passed   bool
	failures []string
}

var buildTiming = regexp.MustCompile(`(docker-info|build-healer|build-connect) finished in (\S+)`)

func readEvents(reader io.Reader) ([]testEvent, error) {
	var events []testEvent
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		var event testEvent
		if json.Unmarshal(scanner.Bytes(), &event) == nil {
			events = append(events, event)
		}
		// A cancelled process can leave a partial final JSON line. A missing
		// final package result still makes the report incomplete, never passed.
	}
	return events, scanner.Err()
}

func summarize(events []testEvent) report {
	groups := make(map[string]float64)
	builds := make(map[string]string)
	failures := make(map[string]bool)
	var suiteStarted, firstGroup time.Time
	var packageResult *testEvent
	for _, event := range events {
		if event.Action == "run" && event.Test == "TestIntegration" {
			suiteStarted = event.Time
		}
		if event.Action == "run" && strings.HasPrefix(event.Test, "TestIntegration/") && firstGroup.IsZero() {
			firstGroup = event.Time
		}
		if event.Action == "pass" || event.Action == "fail" {
			if event.Test == "" {
				packageResult = &event
			} else if strings.HasPrefix(event.Test, "TestIntegration/") && strings.Count(event.Test, "/") == 1 {
				groups[strings.TrimPrefix(event.Test, "TestIntegration/")] = event.Elapsed
			}
			if event.Action == "fail" && event.Test != "" {
				failures[event.Test] = true
			}
		}
		if match := buildTiming.FindStringSubmatch(event.Output); match != nil {
			builds[match[1]] = match[2]
		}
	}
	result := report{}
	for test := range failures {
		leaf := true
		for other := range failures {
			if strings.HasPrefix(other, test+"/") {
				leaf = false
				break
			}
		}
		if leaf {
			result.failures = append(result.failures, test)
		}
	}
	sort.Strings(result.failures)
	result.passed = packageResult != nil && packageResult.Action == "pass" && len(result.failures) == 0
	status := "Incomplete"
	if packageResult != nil {
		status = "Failed"
		if result.passed {
			status = "Passed"
		}
	}
	var output strings.Builder
	fmt.Fprintf(&output, "## Kafka Connect integration\n\nResult: **%s**\n", status)
	if packageResult != nil {
		fmt.Fprintf(&output, "Go test time: **%.2fs** (includes image preparation and cleanup).\n", packageResult.Elapsed)
	}
	if !suiteStarted.IsZero() && !firstGroup.IsZero() {
		fmt.Fprintf(&output, "Suite setup: **%.2fs**.\n", firstGroup.Sub(suiteStarted).Seconds())
	}
	if len(builds) > 0 {
		fmt.Fprintln(&output, "\n| Image preparation | Time |\n| --- | --- |")
		for _, name := range []string{"docker-info", "build-healer", "build-connect"} {
			if duration, exists := builds[name]; exists {
				fmt.Fprintf(&output, "| %s | %s |\n", name, duration)
			}
		}
		fmt.Fprintln(&output, "\nImage timings are per command; suite setup measures wall time.")
	}
	if len(groups) > 0 {
		var names []string
		for name := range groups {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool {
			if groups[names[i]] == groups[names[j]] {
				return names[i] < names[j]
			}
			return groups[names[i]] > groups[names[j]]
		})
		fmt.Fprintln(&output, "\n| Test group (slowest first) | Seconds |\n| --- | ---: |")
		for _, name := range names {
			fmt.Fprintf(&output, "| %s | %.2f |\n", strings.ReplaceAll(name, "|", `\|`), groups[name])
		}
	}
	if len(result.failures) > 0 {
		fmt.Fprint(&output, "\nFailed tests:\n\n")
		for _, test := range result.failures {
			fmt.Fprintf(&output, "- `%s`\n", test)
		}
	}
	if packageResult == nil {
		fmt.Fprintln(&output, "\nNo final package result was recorded. Check image preparation, runner cancellation, and the raw test log.")
	}
	fmt.Fprintln(&output, "\nFull JSON test results and container/build logs are in the **integration-diagnostics** artifact.")
	result.markdown = output.String()
	return result
}

func run(arguments []string, stdout, stderr io.Writer, summaryPath string, annotations bool) int {
	if len(arguments) != 1 {
		fmt.Fprintln(stderr, "ERROR [CI_REPORT_INPUT] Usage: integration-report TEST_RESULTS_JSONL")
		return 2
	}
	var events []testEvent
	file, err := os.Open(arguments[0])
	if err == nil {
		events, err = readEvents(file)
		file.Close()
	}
	if err != nil {
		fmt.Fprintf(stderr, "ERROR [CI_REPORT_INPUT] Cannot read complete test results: %v\n", err)
	}
	result := summarize(events)
	fmt.Fprint(stdout, result.markdown)
	if summaryPath != "" {
		file, writeError := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if writeError == nil {
			_, writeError = io.WriteString(file, result.markdown)
			file.Close()
		}
		if writeError != nil {
			fmt.Fprintf(stderr, "ERROR [CI_REPORT_OUTPUT] Cannot write job summary: %v\n", writeError)
			return 1
		}
	}
	if result.passed && err == nil {
		return 0
	}
	failures := result.failures
	if len(failures) == 0 {
		failures = []string{"Integration run has no successful final package result"}
	}
	for _, test := range failures {
		message := test + ". Inspect the test output and integration-diagnostics artifact."
		if annotations {
			message = strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(message)
			fmt.Fprintf(stderr, "::error title=CI_INTEGRATION_FAILED::%s\n", message)
		} else {
			fmt.Fprintf(stderr, "ERROR [CI_INTEGRATION_FAILED] %s\n", message)
		}
	}
	return 1
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv("GITHUB_STEP_SUMMARY"), os.Getenv("GITHUB_ACTIONS") == "true"))
}
