package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	defaultHealerImage  = "kafka-connect-healer:integration"
	defaultConnectImage = "kafka-connect-healer-connect:integration"
)

var integrationImages struct {
	sync.Once
	err error
}

// Called only by a selected, non-short integration test. In particular, -list,
// -short, and runs selecting only setup unit tests never invoke Docker.
func prepareImages(t *testing.T) {
	t.Helper()
	integrationImages.Do(func() {
		projectDirectory, err := filepath.Abs(filepath.Join("..", ".."))
		if err != nil {
			integrationImages.err = fmt.Errorf("locate project directory: %w", err)
			return
		}
		integrationImages.err = buildTestImages(t.Context(), projectDirectory,
			func(ctx context.Context, directory, step string, arguments ...string) ([]byte, error) {
				t.Logf("preparing integration images: %s", step)
				started := time.Now()
				output, commandErr := runDockerCommand(ctx, directory, arguments...)
				if err := os.MkdirAll(artifactDirectory(), 0o755); err != nil {
					return output, fmt.Errorf("create diagnostics directory: %w", err)
				}
				if err := os.WriteFile(filepath.Join(artifactDirectory(), step+".log"), output, 0o600); err != nil {
					return output, fmt.Errorf("save Docker diagnostics: %w", err)
				}
				t.Logf("%s finished in %s", step, time.Since(started).Round(time.Millisecond))
				return output, commandErr
			})
	})
	if integrationImages.err != nil {
		t.Fatalf("prepare integration images: %v", integrationImages.err)
	}
}

type dockerCommand func(context.Context, string, string, ...string) ([]byte, error)

func buildTestImages(ctx context.Context, directory string, run dockerCommand) error {
	// go test runs this package from tests/integration. Verify the expected
	// inputs before building so a bad working directory cannot build another project.
	for _, path := range []string{"go.mod", "Dockerfile", "tests/integration/testdata/Dockerfile.connect"} {
		info, err := os.Stat(filepath.Join(directory, filepath.FromSlash(path)))
		if err != nil {
			return fmt.Errorf("locate integration build input %q: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("integration build input %q must be a regular file", path)
		}
	}
	steps := []struct {
		name      string
		timeout   time.Duration
		arguments []string
	}{
		{"docker-info", 15 * time.Second, []string{"info", "--format", "{{.ServerVersion}}"}},
		{"build-healer", 5 * time.Minute, []string{"build", "--progress=plain", "--tag", image("HEALER_TEST_IMAGE", defaultHealerImage), "--file", "Dockerfile", "."}},
		{"build-connect", 5 * time.Minute, []string{"build", "--progress=plain", "--tag", image("CONNECT_TEST_IMAGE", defaultConnectImage), "--file", "tests/integration/testdata/Dockerfile.connect", "tests/integration/testdata"}},
	}
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		commandCtx, cancel := context.WithTimeout(ctx, step.timeout)
		output, err := run(commandCtx, directory, step.name, step.arguments...)
		if commandCtx.Err() != nil {
			err = commandCtx.Err()
		}
		cancel()
		if err != nil {
			if step.name == "docker-info" {
				return fmt.Errorf("Docker CLI and a running Docker daemon are required: %w\n%s", err, output)
			}
			return fmt.Errorf("%s failed: %w\n%s", step.name, err, output)
		}
	}
	return nil
}

func runDockerCommand(ctx context.Context, directory string, arguments ...string) ([]byte, error) {
	// Use the CLI to support the production Dockerfile's BuildKit cache mounts.
	// No shell or pipelines: paths and image tags are individual arguments.
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.Dir = directory
	command.Env = append(os.Environ(), "DOCKER_BUILDKIT=1")
	// Bound pipe draining if a Docker CLI child keeps stdout/stderr open after cancellation.
	command.WaitDelay = 5 * time.Second
	return command.CombinedOutput()
}

func artifactDirectory() string {
	if directory := os.Getenv("INTEGRATION_ARTIFACTS"); directory != "" {
		return directory
	}
	return "artifacts"
}
