package connectcluster

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/Entropic-Works/kafka-connect-healer/internal/logging"
)

func TestConfigurationLogValueRedactsCredentialsAndDescribesTLS(t *testing.T) {
	for _, test := range []struct {
		tls                    TLSConfiguration
		endpoint, verification string
	}{
		{TLSConfiguration{}, "http://[::1]:8083", "not applicable (HTTP)"},
		{TLSConfiguration{HTTPS: true}, "https://[::1]:8083", "enabled (system trust store)"},
		{TLSConfiguration{HTTPS: true, InsecureSkipVerify: true}, "https://[::1]:8083", "disabled (insecureSkipVerify)"},
	} {
		for _, structured := range []bool{false, true} {
			var output bytes.Buffer
			var handler slog.Handler = logging.NewConsoleHandler(&output, nil)
			if structured {
				handler = slog.NewJSONHandler(&output, nil)
			}
			configuration := Configuration{Host: "::1", Port: "8083", TLS: test.tls, AuthConfig: AuthConfiguration{Enabled: true, Username: "private-user", Password: "private-password"}}
			slog.New(handler).Info("cluster", "production", configuration)
			for _, field := range []string{test.endpoint, test.verification} {
				if !strings.Contains(output.String(), field) {
					t.Fatalf("missing %q: %s", field, output.String())
				}
			}
			for _, secret := range []string{"private-user", "private-password", "Username", "Password"} {
				if strings.Contains(output.String(), secret) {
					t.Fatalf("configuration log exposed credentials: %s", output.String())
				}
			}
		}
	}
}
