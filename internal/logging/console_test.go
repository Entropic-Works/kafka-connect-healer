package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConsoleEventFormattingAndFiltering(t *testing.T) {
	var output bytes.Buffer
	var level slog.LevelVar
	handler := NewConsoleHandler(&output, &level)
	record := slog.NewRecord(time.Date(2026, 10, 31, 12, 0, 0, 123000000, time.UTC), slog.LevelInfo, "settings updated", 0)
	record.AddAttrs(slog.Duration("duration", 1234567890), slog.String("connector", "name with spaces"),
		slog.Group("Application", slog.Bool("Telemetry", true), slog.Group("Backoff", slog.Bool("Enabled", false))))
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	want := "2026-10-31T12:00:00.123Z INFO  settings updated | duration=1.235s connector=\"name with spaces\"\n    Application:\n        Telemetry: enabled\n        Backoff:\n            Enabled: disabled\n"
	if output.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", output.String(), want)
	}
	record.Attrs(func(attribute slog.Attr) bool {
		if attribute.Key == "duration" && attribute.Value.Duration() != 1234567890 {
			t.Fatal("console changed structured duration")
		}
		return true
	})
	level.Set(slog.LevelWarn)
	slog.New(handler).Info("hidden")
	if output.String() != want {
		t.Fatal("INFO bypassed configured level")
	}
	level.Set(slog.LevelInfo)
	slog.New(handler).With("component", "api").WithGroup("scoped").Info("shown", slog.Group("settings", slog.Int("Count", 2)))
	if !strings.Contains(output.String(), "component=api") || !strings.Contains(output.String(), "    scoped:\n        settings:\n            Count: 2") {
		t.Fatal(output.String())
	}
}

func TestConsoleEventsStayTogether(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(NewConsoleHandler(&output, nil))
	var workers sync.WaitGroup
	for range 40 {
		workers.Go(func() { logger.With("component", "test").Info("summary", slog.Group("Settings", slog.Int("Count", 1))) })
	}
	workers.Wait()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 120 {
		t.Fatalf("lines = %d", len(lines))
	}
	for index := 0; index < len(lines); index += 3 {
		if !strings.Contains(lines[index], " INFO  summary | component=test") || lines[index+1] != "    Settings:" || lines[index+2] != "        Count: 1" {
			t.Fatalf("interleaved event: %q", lines[index:index+3])
		}
	}
}

func TestConsoleEscapesControlCharacters(t *testing.T) {
	var output bytes.Buffer
	slog.New(NewConsoleHandler(&output, nil)).Info("event\nforged", slog.Group("name\nforged", slog.String("field", "value\nforged")))
	if strings.Count(output.String(), "\n") != 3 || !strings.Contains(output.String(), `value\nforged`) {
		t.Fatal(output.String())
	}
}
