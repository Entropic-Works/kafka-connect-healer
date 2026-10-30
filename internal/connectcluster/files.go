package connectcluster

import (
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/Entropic-Works/kafka-connect-healer/internal/yamlconfig"
)

// LoadFiles applies per-cluster defaults, an optional base file, then an optional
// Secret overlay. Omitting the base file starts with the default localhost cluster.
// Values are validated only after both files have been merged.
func LoadFiles(basePath, secretPath string) (map[string]Configuration, error) {
	clusters := make(map[string]Configuration)
	if basePath == "" {
		clusters["default"] = DefaultConfiguration()
	} else {
		if err := decodeFile(basePath, clusters); err != nil {
			return nil, fmt.Errorf("base cluster configuration: %w", err)
		}
	}
	if secretPath != "" {
		if err := decodeFile(secretPath, clusters); err != nil {
			return nil, fmt.Errorf("cluster Secret overlay: %w", err)
		}
	}
	for name, configuration := range clusters {
		if err := ValidateConfiguration(configuration); err != nil {
			return nil, fmt.Errorf("cluster %q: %w", name, err)
		}
	}
	WarnDuplicateEndpoints(clusters)
	return clusters, nil
}

// WarnDuplicateEndpoints compares literal endpoints without DNS lookups or credentials.
func WarnDuplicateEndpoints(clusters map[string]Configuration) {
	seen := make(map[string]string)
	for _, name := range slices.Sorted(maps.Keys(clusters)) {
		configuration := clusters[name]
		host := strings.ToLower(strings.TrimSuffix(configuration.Host, "."))
		if address, err := netip.ParseAddr(configuration.Host); err == nil {
			host = address.String()
		}
		port, _ := strconv.Atoi(configuration.Port) // LoadFiles has already validated it.
		scheme := "http"
		if configuration.TLS.HTTPS {
			scheme = "https"
		}
		endpoint := scheme + "://" + net.JoinHostPort(host, strconv.Itoa(port))
		if first, exists := seen[endpoint]; exists {
			slog.Warn("Connect clusters share an endpoint; independent pollers may issue duplicate restart requests",
				"connect_clusters", []string{first, name}, "endpoint", endpoint)
		} else {
			seen[endpoint] = name
		}
	}
}

func decodeFile(path string, clusters map[string]Configuration) error {
	root, err := yamlconfig.ReadFile(path)
	if err != nil {
		return err
	}
	for i := 0; i < len(root.Content); i += 2 {
		name := root.Content[i].Value
		configuration, exists := clusters[name]
		if !exists {
			configuration = DefaultConfiguration()
		}
		if err := yamlconfig.Decode(root.Content[i+1], &configuration); err != nil {
			return fmt.Errorf("%q: cluster %q: %w", path, name, err)
		}
		clusters[name] = configuration
	}
	return nil
}
