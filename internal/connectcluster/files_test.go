package connectcluster

import (
	"bytes"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFilesWarnsAboutDuplicateEndpoints(t *testing.T) {
	for _, test := range []struct {
		name        string
		base        string
		overlay     string
		wantWarning bool
	}{
		{name: "default endpoints", base: "production: {}\nstaging: {}", wantWarning: true},
		{name: "equivalent host and port", base: "production: {host: CONNECT.local., port: '08083'}\nstaging: {host: connect.local}", wantWarning: true},
		{name: "equivalent IPv6", base: "production: {host: '2001:db8::1'}\nstaging: {host: '2001:0db8:0:0:0:0:0:1'}", wantWarning: true},
		{name: "overlay introduces duplicate", base: "production: {host: production.local}\nstaging: {host: staging.local}", overlay: "staging: {host: production.local}", wantWarning: true},
		{name: "different hosts", base: "production: {host: production.local}\nstaging: {host: staging.local}"},
		{name: "different ports", base: "production: {}\nstaging: {port: '8084'}"},
		{name: "different schemes", base: "production: {}\nstaging: {tls: {https: true}}"},
		{name: "different TLS policies share an endpoint", base: "production: {tls: {https: true, insecureSkipVerify: true}}\nstaging: {tls: {https: true}}", wantWarning: true},
		{name: "empty clusters", base: "{}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })
			overlayPath := ""
			if test.overlay != "" {
				overlayPath = writeConfigFile(t, test.overlay)
			}
			clusters, err := LoadFiles(writeConfigFile(t, test.base), overlayPath)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(logs.String(), "level=WARN"); got != test.wantWarning {
				t.Fatalf("warning = %t, want %t; logs: %s", got, test.wantWarning, logs.String())
			}
			if test.wantWarning && (len(clusters) != 2 || !strings.Contains(logs.String(), "production staging")) {
				t.Fatalf("duplicate endpoints must remain configured and both names must be logged: %s", logs.String())
			}
		})
	}
}

func TestLoadFilesDuplicateWarningOmitsCredentials(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	base := writeConfigFile(t, "production:\n  tls:\n    https: true\n  authConfig: {enabled: true, username: private-user, password: private-password}\nstaging:\n  tls:\n    https: true")
	if _, err := LoadFiles(base, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "level=WARN") || strings.Contains(logs.String(), "private-user") || strings.Contains(logs.String(), "private-password") {
		t.Fatalf("expected a credential-free warning: %s", logs.String())
	}
}

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clusters.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFilesWithoutBaseUsesDefaultCluster(t *testing.T) {
	got, err := LoadFiles("", "")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Configuration{"default": DefaultConfiguration()}
	if !maps.Equal(got, want) {
		t.Fatalf("clusters = %#v, want %#v", got, want)
	}
}

func TestLoadFilesTLSOverlays(t *testing.T) {
	for _, test := range []struct {
		name, base, overlay string
		want                TLSConfiguration
	}{
		{name: "nested settings", base: "production: {tls: {https: true, insecureSkipVerify: true}}", want: TLSConfiguration{HTTPS: true, InsecureSkipVerify: true}},
		{name: "overlay enables bypass", base: "production: {tls: {https: true}}", overlay: "production: {tls: {insecureSkipVerify: true}}", want: TLSConfiguration{HTTPS: true, InsecureSkipVerify: true}},
		{name: "overlay restores verification", base: "production: {tls: {https: true, insecureSkipVerify: true}}", overlay: "production: {tls: {insecureSkipVerify: false}}", want: TLSConfiguration{HTTPS: true}},
		{name: "empty nested overlay", base: "production: {tls: {https: true, insecureSkipVerify: true}}", overlay: "production: {tls: {}}", want: TLSConfiguration{HTTPS: true, InsecureSkipVerify: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			overlay := ""
			if test.overlay != "" {
				overlay = writeConfigFile(t, test.overlay)
			}
			clusters, err := LoadFiles(writeConfigFile(t, test.base), overlay)
			if err != nil || clusters["production"].TLS != test.want {
				t.Fatalf("clusters = %+v, error = %v; want TLS %+v", clusters, err, test.want)
			}
		})
	}
}

func TestLoadFilesOverlayWithoutBase(t *testing.T) {
	overlay := writeConfigFile(t, "default:\n  host: connect.local\nstaging: {}")
	got, err := LoadFiles("", overlay)
	if err != nil {
		t.Fatal(err)
	}
	defaultCluster := DefaultConfiguration()
	defaultCluster.Host = "connect.local"
	want := map[string]Configuration{
		"default": defaultCluster,
		"staging": DefaultConfiguration(),
	}
	if !maps.Equal(got, want) {
		t.Fatalf("clusters = %#v, want %#v", got, want)
	}
}

func TestLoadFilesDefaultsAndPartialOverlay(t *testing.T) {
	base := writeConfigFile(t, `
production:
  host: connect.production.svc
  port: "8443"
  tls:
    https: true
  authConfig:
    enabled: true
    username: base-user
development: {}
`)
	overlay := writeConfigFile(t, `
production:
  authConfig:
    username: secret-user
    password: secret-password
`)
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	want := map[string]Configuration{
		"production": {
			Host: "connect.production.svc", Port: "8443", TLS: TLSConfiguration{HTTPS: true},
			AuthConfig: AuthConfiguration{Enabled: true, Username: "secret-user", Password: "secret-password"},
		},
		"development": DefaultConfiguration(),
	}
	if !maps.Equal(got, want) {
		t.Fatal("cluster overlay lost endpoint settings, credentials, or per-cluster defaults")
	}
	if _, err := LoadFiles(base, ""); err == nil || !strings.Contains(err.Error(), `cluster "production": authConfig.password:`) {
		t.Fatalf("missing credentials error = %v", err)
	}
}

func TestLoadFilesAllowsAuthenticatedHTTPWithSecretOverlay(t *testing.T) {
	base := writeConfigFile(t, "production:\n  host: connect.production.svc\n  authConfig:\n    enabled: true\n    username: connect-user\n")
	overlay := writeConfigFile(t, "production:\n  authConfig:\n    password: connect-password\n")
	clusters, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	want := Configuration{
		Host: "connect.production.svc", Port: "8083",
		AuthConfig: AuthConfiguration{Enabled: true, Username: "connect-user", Password: "connect-password"},
	}
	if len(clusters) != 1 || clusters["production"] != want {
		t.Fatal("HTTP cluster did not retain merged authentication and endpoint defaults")
	}
}

func TestLoadFilesClusterMembershipAndEmptyMappings(t *testing.T) {
	got, err := LoadFiles(writeConfigFile(t, "production:\n  host: connect.production\n"), writeConfigFile(t, "production: {}\nstaging: {}"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Configuration{"production": DefaultConfiguration(), "staging": DefaultConfiguration()}
	production := want["production"]
	production.Host = "connect.production"
	want["production"] = production
	if !maps.Equal(got, want) {
		t.Fatal("loader added an implicit cluster or lost existing values")
	}
	empty, err := LoadFiles(writeConfigFile(t, "{}"), "")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty mapping = %v, error = %v, want no clusters", empty, err)
	}
}

func TestLoadFilesOverlayCanExplicitlyClearValues(t *testing.T) {
	base := writeConfigFile(t, "production:\n  tls:\n    https: true\n  authConfig:\n    enabled: true\n    username: user\n    password: password")
	overlay := writeConfigFile(t, "production:\n  tls:\n    https: false\n  authConfig:\n    enabled: false\n    username: \"\"\n    password: \"\"")
	got, err := LoadFiles(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if got["production"] != DefaultConfiguration() {
		t.Fatal("explicit false and empty strings did not clear existing values")
	}
}

func TestLoadFilesRejectsInvalidYAMLInEitherFile(t *testing.T) {
	for _, contents := range []string{
		"", "# comment", "null", "[]", "production: [", "{}\n---\n{}", "{}\n---",
		"production: null", "production: []", "production: scalar",
		"production: {}\nproduction: {}",
		"production:\n  host: first\n  host: second",
		"production:\n  authConfig:\n    password: first\n    password: second",
		"production:\n  host: null", "production:\n  tls:\n    https:",
		"production:\n  typo: true", "production:\n  authConfig:\n    typo: true",
		"production:\n  tls:\n    https: perhaps",
		"production: {tls: null}", "production: {tls: []}",
		"production: {tls: {https: null}}", "production: {tls: {insecureSkipVerify: null}}",
		"production: {tls: {https: true, https: false}}",
		"production: {tls: {https: true, insecureSkipVerify: true, insecureSkipVerify: false}}",
		"production: {tls: {insecureSkipVerify: true}}",
		"production: {https: true}", "production: {https: false}",
		"production: {tls: {https: true, caBundlePath: unsupported}}",
		"production: {tls: {https: true, clientCertPath: unsupported}}",
		"production: {tls: {https: true, clientKeyPath: unsupported}}",
		"production:\n  host: &host connect\n  port: *host",
		"production:\n  <<: {host: connect}", "1: {}",
	} {
		t.Run(contents, func(t *testing.T) {
			invalid := writeConfigFile(t, contents)
			for _, paths := range [][2]string{{invalid, ""}, {writeConfigFile(t, "production: {}"), invalid}} {
				got, err := LoadFiles(paths[0], paths[1])
				if err == nil || got != nil {
					t.Fatal("invalid file was accepted or returned partial clusters")
				}
			}
		})
	}
}

func TestLoadFilesErrorsDoNotExposeCredentials(t *testing.T) {
	const sensitive = "credential-that-must-not-be-logged"
	for _, contents := range []string{
		"production:\n  authConfig:\n    password: [" + sensitive + "]",
		"production:\n  authConfig:\n    password: first\n    password: " + sensitive,
		"production:\n  tls:\n    https: " + sensitive,
		"production:\n  host: " + sensitive + "/invalid",
	} {
		invalid := writeConfigFile(t, contents)
		for _, paths := range [][2]string{{invalid, ""}, {writeConfigFile(t, "production: {}"), invalid}} {
			got, err := LoadFiles(paths[0], paths[1])
			if got != nil || err == nil || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("expected a safe configuration error, got %v", err)
			}
		}
	}
}

func TestLoadFilesRequiresEverySpecifiedFile(t *testing.T) {
	valid := writeConfigFile(t, "production: {}")
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for _, paths := range [][2]string{{"", missing}, {missing, ""}, {valid, missing}, {t.TempDir(), ""}} {
		if got, err := LoadFiles(paths[0], paths[1]); err == nil || got != nil {
			t.Fatal("missing or unreadable file was accepted")
		}
	}
}
