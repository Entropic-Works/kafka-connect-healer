package connectcluster

import (
	"strings"
	"testing"
)

func TestValidateConnectHost(t *testing.T) {
	for _, host := range []string{
		"localhost", "kafka-connect", "connect.kafka.svc.cluster.local", "connect.example.test.",
		"CONNECT.example.test", "127.0.0.1", "2001:db8::1", "::1", "fe80::1%eth0",
	} {
		t.Run("valid/"+host, func(t *testing.T) {
			configuration := DefaultConfiguration()
			configuration.Host = host
			if err := ValidateConfiguration(configuration); err != nil {
				t.Fatalf("valid host rejected: %v", err)
			}
		})
	}
	for _, host := range []string{
		"", " ", "http://localhost", "https://connect.example.test", "localhost:8083", "connect/path", "connect\\path",
		"connect?query", "connect#fragment", "user@connect", "[::1]", "[::1]:8083", "2001:db8::invalid",
		" connect", "connect ", "con nect", "connect\t", "connect\n", "con\x00nect", "con\u00a0nect",
		"connect%2Fpath", ".", "connect..example", "-connect", "connect-", "con_nect", "connect..",
		strings.Repeat("a", 64) + ".example", strings.Repeat("a.", 128) + "test",
	} {
		t.Run("invalid/"+host, func(t *testing.T) {
			configuration := DefaultConfiguration()
			configuration.Host = host
			if err := ValidateConfiguration(configuration); err == nil || !strings.HasPrefix(err.Error(), "host:") {
				t.Fatalf("host validation error = %v", err)
			}
		})
	}
}

func TestValidatePortAndAuthentication(t *testing.T) {
	for _, test := range []struct {
		configuration Configuration
		want          string
	}{
		{Configuration{Host: "localhost", Port: "65536"}, "port:"},
		{Configuration{Host: "localhost", Port: "not-a-port"}, "port:"},
		{Configuration{Host: "localhost", Port: "0"}, "port:"},
		{Configuration{Host: "localhost", Port: "+8083"}, "port:"},
		{Configuration{Host: "localhost", Port: "-8083"}, "port:"},
		{Configuration{Host: "localhost", Port: " 8083"}, "port:"},
		{Configuration{Host: "localhost", Port: "8083 "}, "port:"},
		{Configuration{Host: "localhost", Port: "８０８３"}, "port:"},
		{Configuration{Host: "localhost", Port: ""}, "port:"},
		{Configuration{Host: "localhost", Port: "0008083"}, ""},
		{Configuration{Host: "localhost", Port: "1"}, ""},
		{Configuration{Host: "localhost", Port: "65535"}, ""},
		{Configuration{Host: "localhost", Port: "8083", TLS: TLSConfiguration{InsecureSkipVerify: true}}, "tls.insecureSkipVerify:"},
		{Configuration{Host: "localhost", Port: "8443", TLS: TLSConfiguration{HTTPS: true, InsecureSkipVerify: true}}, ""},
		{Configuration{Host: "localhost", Port: "8083", AuthConfig: AuthConfiguration{Enabled: true}}, "authConfig.username:"},
		{Configuration{Host: "localhost", Port: "8083", AuthConfig: AuthConfiguration{Enabled: true, Username: "user"}}, "authConfig.password:"},
		{Configuration{Host: "localhost", Port: "8083", AuthConfig: AuthConfiguration{Enabled: true, Username: "team:user", Password: "secret"}}, "authConfig.username:"},
		{Configuration{Host: "localhost", Port: "8083", AuthConfig: AuthConfiguration{Enabled: true, Username: " ", Password: "secret"}}, "authConfig.username:"},
		{Configuration{Host: "localhost", Port: "8083", AuthConfig: AuthConfiguration{Enabled: true, Username: "user", Password: "secret"}}, ""},
		{Configuration{Host: "localhost", Port: "8083", TLS: TLSConfiguration{HTTPS: true}, AuthConfig: AuthConfiguration{Enabled: true, Password: "secret"}}, "authConfig.username:"},
		{Configuration{Host: "localhost", Port: "8083", TLS: TLSConfiguration{HTTPS: true}, AuthConfig: AuthConfiguration{Enabled: true, Username: "user"}}, "authConfig.password:"},
		{Configuration{Host: "localhost", Port: "8083", TLS: TLSConfiguration{HTTPS: true}, AuthConfig: AuthConfiguration{Enabled: true, Username: "team:user", Password: "secret"}}, "authConfig.username:"},
		{Configuration{Host: "localhost", Port: "8083", TLS: TLSConfiguration{HTTPS: true}, AuthConfig: AuthConfiguration{Enabled: true, Username: " ", Password: "secret"}}, "authConfig.username:"},
		{Configuration{Host: "localhost", Port: "8083", TLS: TLSConfiguration{HTTPS: true}, AuthConfig: AuthConfiguration{Enabled: true, Username: "user", Password: "secret"}}, ""},
	} {
		err := ValidateConfiguration(test.configuration)
		if test.want == "" {
			if err != nil {
				t.Fatalf("valid cluster rejected: %v", err)
			}
		} else if err == nil || !strings.HasPrefix(err.Error(), test.want) {
			t.Fatalf("validation error = %v, want %q", err, test.want)
		}
	}
}
