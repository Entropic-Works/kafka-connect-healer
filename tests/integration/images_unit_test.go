package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
			commands := make(map[string][]string)
			var commandMutex sync.Mutex
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
				commandMutex.Lock()
				steps = append(steps, step)
				commands[step] = slices.Clone(arguments)
				commandMutex.Unlock()
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(steps) != 3 || steps[0] != "docker-info" {
				t.Fatalf("unexpected preparation order: %v", steps)
			}
			want := map[string][]string{
				"docker-info":   {"info", "--format", "{{.ServerVersion}}"},
				"build-healer":  {"build", "--progress=plain", "--tag", healerTag, "--file", "Dockerfile", "."},
				"build-connect": {"build", "--progress=plain", "--tag", connectTag, "--file", "tests/integration/testdata/Dockerfile.connect", "tests/integration/testdata"},
			}
			for step := range want {
				if !slices.Equal(commands[step], want[step]) {
					t.Errorf("%s command: got %v, want %v", step, commands[step], want[step])
				}
			}
		})
	}
}

func TestBuildTestImagesStopsOnFailure(t *testing.T) {
	for _, failedStep := range []string{"docker-info", "build-healer", "build-connect"} {
		t.Run(failedStep, func(t *testing.T) {
			sentinel := errors.New("command failed")
			var calls atomic.Int32
			err := buildTestImages(t.Context(), buildInputs(t), func(_ context.Context, _, step string, _ ...string) ([]byte, error) {
				calls.Add(1)
				if step == failedStep {
					return []byte("useful Docker failure output"), sentinel
				}
				return nil, nil
			})
			if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "useful Docker failure output") {
				t.Fatalf("lost build failure or its diagnostics: %v", err)
			}
			if failedStep == "docker-info" && calls.Load() != 1 {
				t.Fatalf("built images after daemon check failed: %d calls", calls.Load())
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

func TestBuildTestImagesOverlapAndCancelOnFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	otherBuildStarted := make(chan struct{})
	otherBuildCanceled := make(chan struct{})
	sentinel := errors.New("application build failed")
	err := buildTestImages(ctx, buildInputs(t), func(commandCtx context.Context, _, step string, _ ...string) ([]byte, error) {
		switch step {
		case "docker-info":
			return nil, nil
		case "build-healer":
			select {
			case <-otherBuildStarted:
				return []byte("compiler diagnostics"), sentinel
			case <-commandCtx.Done():
				return nil, commandCtx.Err()
			}
		case "build-connect":
			close(otherBuildStarted)
			<-commandCtx.Done()
			close(otherBuildCanceled)
			return nil, commandCtx.Err()
		default:
			t.Errorf("unexpected step %s", step)
			return nil, nil
		}
	})
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "compiler diagnostics") {
		t.Fatalf("independent builds did not overlap or original failure was lost: %v", err)
	}
	select {
	case <-otherBuildCanceled:
	default:
		t.Fatal("failed build did not cancel and await the other build")
	}
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
