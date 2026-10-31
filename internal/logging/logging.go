package logging

import (
	"log/slog"
	"os"
)

var consoleLevel slog.LevelVar

func Configure() {
	slog.SetDefault(slog.New(NewConsoleHandler(os.Stderr, &consoleLevel)))
}

// SetLevel applies configuration changes before their summary is logged.
func SetLevel(level slog.Level) {
	consoleLevel.Set(level)
}
