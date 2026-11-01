package logging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// consoleHandler delegates attribute escaping and scoping to slog.TextHandler.
// Its shared lock keeps an entire event, including indented sections, together.
type consoleHandler struct {
	slog.Handler
	output *consoleOutput
	groups []string
}

type consoleOutput struct {
	sync.Mutex
	writer     io.Writer
	attributes bytes.Buffer
}

func NewConsoleHandler(writer io.Writer, level slog.Leveler) slog.Handler {
	output := &consoleOutput{writer: writer}
	handler := slog.NewTextHandler(&output.attributes, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attribute slog.Attr) slog.Attr {
			if len(groups) == 0 && (attribute.Key == slog.TimeKey || attribute.Key == slog.LevelKey || attribute.Key == slog.MessageKey) {
				return slog.Attr{}
			}
			switch attribute.Value.Kind() {
			case slog.KindDuration:
				duration := attribute.Value.Duration()
				if duration >= time.Second {
					duration = duration.Round(time.Millisecond)
				} else if duration >= time.Millisecond {
					duration = duration.Round(time.Microsecond)
				}
				attribute.Value = slog.StringValue(duration.String())
			case slog.KindTime:
				attribute.Value = slog.StringValue(attribute.Value.Time().Format("2006-01-02T15:04:05.000Z07:00"))
			}
			return attribute
		},
	})
	return &consoleHandler{Handler: handler, output: output}
}

func (handler *consoleHandler) Handle(ctx context.Context, record slog.Record) error {
	handler.output.Lock()
	defer handler.output.Unlock()
	handler.output.attributes.Reset()
	attributes := slog.NewRecord(time.Time{}, record.Level, "", record.PC)
	var sections []slog.Attr
	record.Attrs(func(attribute slog.Attr) bool {
		attribute.Value = attribute.Value.Resolve()
		if attribute.Value.Kind() == slog.KindGroup {
			sections = append(sections, attribute)
		} else {
			attributes.AddAttrs(attribute)
		}
		return true
	})
	if err := handler.Handler.Handle(ctx, attributes); err != nil {
		return err
	}
	var event bytes.Buffer
	fmt.Fprintf(&event, "%s %-5s %s", record.Time.Format("2006-01-02T15:04:05.000Z07:00"), record.Level, consoleText(record.Message))
	if inline := strings.TrimSpace(handler.output.attributes.String()); inline != "" {
		fmt.Fprintf(&event, " | %s", inline)
	}
	event.WriteByte('\n')
	if len(sections) > 0 {
		for index := len(handler.groups) - 1; index >= 0; index-- {
			sections = []slog.Attr{slog.GroupAttrs(handler.groups[index], sections...)}
		}
		for _, section := range sections {
			writeSection(&event, section, 1)
		}
	}
	_, err := handler.output.writer.Write(event.Bytes())
	return err
}

func (handler *consoleHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	return &consoleHandler{Handler: handler.Handler.WithAttrs(attributes), output: handler.output, groups: handler.groups}
}

func (handler *consoleHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return handler
	}
	groups := append(append([]string(nil), handler.groups...), name)
	return &consoleHandler{Handler: handler.Handler.WithGroup(name), output: handler.output, groups: groups}
}

func writeSection(output *bytes.Buffer, attribute slog.Attr, depth int) {
	attribute.Value = attribute.Value.Resolve()
	if attribute.Equal(slog.Attr{}) {
		return
	}
	indent := strings.Repeat("    ", depth)
	if attribute.Value.Kind() == slog.KindGroup {
		if len(attribute.Value.Group()) == 0 {
			return
		}
		if attribute.Key != "" {
			fmt.Fprintf(output, "%s%s:\n", indent, consoleText(attribute.Key))
			depth++
		}
		for _, child := range attribute.Value.Group() {
			writeSection(output, child, depth)
		}
		return
	}
	value := attribute.Value.String()
	if attribute.Value.Kind() == slog.KindBool {
		value = "disabled"
		if attribute.Value.Bool() {
			value = "enabled"
		}
	}
	fmt.Fprintf(output, "%s%s: %s\n", indent, consoleText(attribute.Key), consoleText(value))
}

func consoleText(value string) string {
	if strings.ContainsFunc(value, unicode.IsControl) {
		return strconv.Quote(value)
	}
	return value
}
