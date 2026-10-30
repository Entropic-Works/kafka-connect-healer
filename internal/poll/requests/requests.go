package requests

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Entropic-Works/kafka-connect-healer/internal/connectcluster"
)

type ConnectAPI struct {
	HTTPClient *http.Client
	BaseURL    string
	Auth       connectcluster.AuthConfiguration
}

// NewConnectAPI creates the HTTP client used for one cluster's status and restart requests.
func NewConnectAPI(configuration connectcluster.Configuration, requestTimeout time.Duration) ConnectAPI {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: configuration.TLS.InsecureSkipVerify}
	client := &http.Client{
		Transport: transport,
		Timeout:   requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	scheme := "http"
	if configuration.TLS.HTTPS {
		scheme = "https"
	}

	return ConnectAPI{
		HTTPClient: client,
		BaseURL: (&url.URL{
			Scheme: scheme,
			Host:   net.JoinHostPort(configuration.Host, configuration.Port),
		}).String(),
		Auth: configuration.AuthConfig,
	}
}

func (connect ConnectAPI) NewRequest(ctx context.Context, method string, requestURL string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, requestURL, nil)
	if err != nil {
		return nil, err
	}

	if connect.Auth.Enabled {
		request.SetBasicAuth(connect.Auth.Username, connect.Auth.Password)
	}

	return request, nil
}
