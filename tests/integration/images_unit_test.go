package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func buildInputs(t *testing.T) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "project with spaces")
	for _, path := range []string{"go.mod", "Dockerfile", "tests/integration/testdata/Dockerfile.connect"} {
		filename := filepath.Join(directory, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte("test build input"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return directory
}

func TestBuildTestImages(t *testing.T) {
	for _, customTags := range []bool{false, true} {
		name := "default_tags"
		if customTags {
			name = "custom_tags"
		}
		t.Run(name, func(t *testing.T) {
			healerTag, connectTag := defaultHealerImage, defaultConnectImage
			t.Setenv("HEALER_TEST_IMAGE", "")
			t.Setenv("CONNECT_TEST_IMAGE", "")
			if customTags {
				healerTag, connectTag = "local/healer:test", "local/connect:test"
				t.Setenv("HEALER_TEST_IMAGE", healerTag)
				t.Setenv("CONNECT_TEST_IMAGE", connectTag)
			}
			directory := buildInputs(t)
			var steps []string
			var commands [][]string
			err := buildTestImages(t.Context(), directory, func(ctx context.Context, actualDirectory, step string, arguments ...string) ([]byte, error) {
				if actualDirectory != directory {
					t.Errorf("working directory: got %q, want %q", actualDirectory, directory)
				}
				deadline, exists := ctx.Deadline()
				limit := 5 * time.Minute
				if step == "docker-info" {
					limit = 15 * time.Second
				}
				if !exists || time.Until(deadline) <= 0 || time.Until(deadline) > limit {
					t.Errorf("%s has no bounded %v deadline: %v", step, limit, deadline)
				}
				steps = append(steps, step)
				commands = append(commands, slices.Clone(arguments))
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(steps, []string{"docker-info", "build-healer", "build-connect"}) {
				t.Fatalf("unexpected preparation order: %v", steps)
			}
			want := [][]string{
				{"info", "--format", "{{.ServerVersion}}"},
				{"build", "--progress=plain", "--tag", healerTag, "--file", "Dockerfile", "."},
				{"build", "--progress=plain", "--tag", connectTag, "--file", "tests/integration/testdata/Dockerfile.connect", "tests/integration/testdata"},
			}
			for index := range want {
				if !slices.Equal(commands[index], want[index]) {
					t.Errorf("%s command: got %v, want %v", steps[index], commands[index], want[index])
				}
			}
		})
	}
}

func TestBuildTestImagesStopsOnFailure(t *testing.T) {
	for index, failedStep := range []string{"docker-info", "build-healer", "build-connect"} {
		t.Run(failedStep, func(t *testing.T) {
			sentinel := errors.New("command failed")
			calls := 0
			err := buildTestImages(t.Context(), buildInputs(t), func(_ context.Context, _, step string, _ ...string) ([]byte, error) {
				calls++
				if step == failedStep {
					return []byte("useful Docker failure output"), sentinel
				}
				return nil, nil
			})
			if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "useful Docker failure output") {
				t.Fatalf("lost build failure or its diagnostics: %v", err)
			}
			if calls != index+1 {
				t.Fatalf("continued building after %s failed: %d calls", failedStep, calls)
			}
			if failedStep == "docker-info" && !strings.Contains(err.Error(), "running Docker daemon") {
				t.Fatalf("missing actionable Docker prerequisite error: %v", err)
			}
		})
	}
}

func TestBuildTestImagesCancellation(t *testing.T) {
	t.Run("before_preparation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := buildTestImages(ctx, buildInputs(t), func(context.Context, string, string, ...string) ([]byte, error) {
			t.Error("canceled preparation invoked Docker")
			return nil, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want cancellation", err)
		}
	})
	t.Run("during_command", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		err := buildTestImages(ctx, buildInputs(t), func(context.Context, string, string, ...string) ([]byte, error) {
			calls++
			cancel()
			return []byte("build interrupted"), errors.New("process killed")
		})
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("cancellation was not preserved: %v (%d calls)", err, calls)
		}
	})
	t.Run("parent_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		parentDeadline, _ := ctx.Deadline()
		err := buildTestImages(ctx, buildInputs(t), func(commandCtx context.Context, _, _ string, _ ...string) ([]byte, error) {
			deadline, _ := commandCtx.Deadline()
			if !deadline.Equal(parentDeadline) {
				t.Errorf("command extends parent deadline: %v > %v", deadline, parentDeadline)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBuildTestImagesRejectsWrongDirectory(t *testing.T) {
	for _, invalid := range []string{"missing_inputs", "directory_instead_of_dockerfile"} {
		t.Run(invalid, func(t *testing.T) {
			directory := t.TempDir()
			if invalid == "directory_instead_of_dockerfile" {
				if err := os.WriteFile(filepath.Join(directory, "go.mod"), nil, 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(directory, "Dockerfile"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			err := buildTestImages(t.Context(), directory, func(context.Context, string, string, ...string) ([]byte, error) {
				t.Error("invalid build inputs invoked Docker")
				return nil, nil
			})
			if err == nil {
				t.Fatal("invalid project directory accepted")
			}
		})
	}
}

func TestArtifactDirectory(t *testing.T) {
	t.Setenv("INTEGRATION_ARTIFACTS", "")
	if got := artifactDirectory(); got != "artifacts" {
		t.Fatalf("default artifact directory: %q", got)
	}
	directory := t.TempDir()
	t.Setenv("INTEGRATION_ARTIFACTS", directory)
	if got := artifactDirectory(); got != directory {
		t.Fatalf("custom artifact directory: %q", got)
	}
}
