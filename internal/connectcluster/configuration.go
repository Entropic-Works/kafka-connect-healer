// Package connectcluster owns Kafka Connect cluster configuration.
package connectcluster

type TLSConfiguration struct {
	HTTPS              bool `json:"https" yaml:"https"`
	InsecureSkipVerify bool `json:"insecureSkipVerify" yaml:"insecureSkipVerify"`
}

type AuthConfiguration struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
}

// Configuration contains endpoint settings for one named cluster.
// The name is the key in the cluster configuration file.
type Configuration struct {
	Host       string            `json:"host" yaml:"host"`
	Port       string            `json:"port" yaml:"port"`
	TLS        TLSConfiguration  `json:"tls" yaml:"tls"`
	AuthConfig AuthConfiguration `json:"authConfig" yaml:"authConfig"`
}

func DefaultConfiguration() Configuration {
	return Configuration{Host: "localhost", Port: "8083"}
}
