package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollectLicensesFollowsImports(t *testing.T) {
	project := licenseFixture(t)
	for _, includeUnused := range []bool{false, true} {
		name := "initial"
		if includeUnused {
			name = "additional dependency"
			writeLicenseFixtureFile(t, project, "cmd/kafka-connect-healer/main.go", "package main\nimport (_ \"example.test/first\"; _ \"example.test/unused\")\nfunc main() {}\n")
		}
		t.Run(name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "notices with spaces")
			if data, err := runLicenseCollector(t, project, output); err != nil {
				t.Fatalf("collect licenses: %v\n%s", err, data)
			}
			for path, want := range map[string]string{
				"example.test/first/LICENSE":          "first license",
				"example.test/first/legal/NOTICE.txt": "first notice",
				"example.test/second/COPYING":         "second license",
			} {
				got, err := os.ReadFile(filepath.Join(output, path))
				if err != nil || string(got) != want {
					t.Fatalf("%s = %q, %v; want %q", path, got, err, want)
				}
			}
			manifest, err := os.ReadFile(filepath.Join(output, "modules.txt"))
			if err != nil {
				t.Fatal(err)
			}
			want := "example.test/first v1.0.0\nexample.test/second v1.0.0\n"
			if includeUnused {
				want += "example.test/unused v1.0.0\n"
			}
			if string(manifest) != want {
				t.Fatalf("module manifest = %q, want %q", manifest, want)
			}
			_, err = os.Stat(filepath.Join(output, "example.test/unused/LICENSE"))
			if includeUnused && err != nil || !includeUnused && !os.IsNotExist(err) {
				t.Fatalf("unused module license: %v; imported = %v", err, includeUnused)
			}
		})
	}
}

func TestCollectLicensesRejectsMissingNotices(t *testing.T) {
	project := licenseFixture(t)
	if err := os.Remove(filepath.Join(project, "deps/second/COPYING")); err != nil {
		t.Fatal(err)
	}
	data, err := runLicenseCollector(t, project, filepath.Join(t.TempDir(), "licenses"))
	if err == nil || !strings.Contains(string(data), "no license or notice found for example.test/second") {
		t.Fatalf("missing license was not reported: %v\n%s", err, data)
	}
}

func TestCollectLicensesPreservesExistingOutput(t *testing.T) {
	project := licenseFixture(t)
	output := t.TempDir()
	marker := filepath.Join(output, "existing")
	if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if data, err := runLicenseCollector(t, project, output); err == nil {
		t.Fatalf("existing directory accepted: %s", data)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatal("existing output was modified")
	}
}

func licenseFixture(t *testing.T) string {
	t.Helper()
	project := filepath.Join(t.TempDir(), "project with spaces")
	writeLicenseFixtureFile(t, project, "go.mod", `module example.test/application

go 1.27.2

require (
  example.test/first v1.0.0
  example.test/second v1.0.0
  example.test/unused v1.0.0
)

replace example.test/first => ./deps/first
replace example.test/second => ./deps/second
replace example.test/unused => ./deps/unused
`)
	for _, module := range []string{"first", "second", "unused"} {
		writeLicenseFixtureFile(t, project, "deps/"+module+"/go.mod", "module example.test/"+module+"\n\ngo 1.27.2\n")
		writeLicenseFixtureFile(t, project, "deps/"+module+"/dep.go", "package dep\n")
	}
	writeLicenseFixtureFile(t, project, "deps/first/dep.go", "package dep\nimport _ \"example.test/second\"\n")
	writeLicenseFixtureFile(t, project, "deps/first/LICENSE", "first license")
	writeLicenseFixtureFile(t, project, "deps/first/legal/NOTICE.txt", "first notice")
	writeLicenseFixtureFile(t, project, "deps/second/COPYING", "second license")
	writeLicenseFixtureFile(t, project, "deps/unused/LICENSE", "unused license")
	writeLicenseFixtureFile(t, project, "cmd/kafka-connect-healer/main.go", "package main\nimport _ \"example.test/first\"\nfunc main() {}\n")
	return project
}

func writeLicenseFixtureFile(t *testing.T, project, path, contents string) {
	t.Helper()
	path = filepath.Join(project, path)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func runLicenseCollector(t *testing.T, project, output string) ([]byte, error) {
	t.Helper()
	script, err := filepath.Abs("collect-licenses.sh")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "sh", script, output)
	command.Dir = project
	command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local", "GOFLAGS=-mod=readonly")
	return command.CombinedOutput()
}
