package connectcluster

import (
	"log/slog"
	"net"
)

// LogValue describes the endpoint and its security policy without credentials.
func (configuration Configuration) LogValue() slog.Value {
	scheme, verification := "http", "not applicable (HTTP)"
	if configuration.TLS.HTTPS {
		scheme, verification = "https", "enabled (system trust store)"
		if configuration.TLS.InsecureSkipVerify {
			verification = "disabled (insecureSkipVerify)"
		}
	}
	return slog.GroupValue(
		slog.String("Endpoint", scheme+"://"+net.JoinHostPort(configuration.Host, configuration.Port)),
		slog.Bool("Authentication", configuration.AuthConfig.Enabled),
		slog.String("TLS verification", verification),
	)
}
