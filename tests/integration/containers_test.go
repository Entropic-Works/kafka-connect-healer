package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	apiUsername     = "healer-test"
	apiPassword     = "healer-test-only"
	connectUsername = "connect-test"
	connectPassword = "connect-test-only"
	baselinePatch   = `{"pollingBehavior":{"interval":"500ms","restartFailedTasks":false,"backoff":{"enabled":true,"baseDelay":"1s","maxDelay":"2s","exponential":true}},"communicationConfig":{"requestTimeout":"2s"},"loggingConfig":{"level":"DEBUG"}}`
	applicationYAML = `pollingBehavior:
  interval: 500ms
  restartFailedTasks: false
  backoff:
    enabled: true
    baseDelay: 1s
    maxDelay: 2s
    exponential: true
communicationConfig:
  requestTimeout: 2s
apiConfig:
  authConfig:
    enabled: true
    username: healer-test
    password: healer-test-only
loggingConfig:
  level: DEBUG
`
)

// Black-box tests deliberately use the packaged application, not its internals.
// Each suite owns a fresh network; no fixed names, host ports, or host mounts.
type suite struct {
	network   *testcontainers.DockerNetwork
	broker    *testcontainers.DockerContainer
	primary   *worker
	secondary *worker
	healer    *testcontainers.DockerContainer
	api       string
	client    *http.Client
	ca        []byte
	sequence  int
}

type worker struct {
	container *testcontainers.DockerContainer
	endpoint  string
	client    *http.Client
}

func image(variable, fallback string) string {
	if value := os.Getenv(variable); value != "" {
		return value
	}
	return fallback
}

func file(path, contents string) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{Reader: strings.NewReader(contents), ContainerFilePath: path, FileMode: 0o644}
}

func runContainer(t *testing.T, name, image string, options ...testcontainers.ContainerCustomizer) *testcontainers.DockerContainer {
	t.Helper()
	container, err := testcontainers.Run(t.Context(), image, options...)
	// Register even when startup failed; Run can return a partially created container.
	testcontainers.CleanupContainer(t, container)
	if container != nil {
		t.Cleanup(func() { saveLogs(t, name, container) })
	}
	if err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	return container
}

func saveLogs(t *testing.T, name string, container *testcontainers.DockerContainer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	reader, err := container.Logs(ctx)
	if err != nil {
		t.Logf("read %s diagnostics: %v", name, err)
		return
	}
	defer reader.Close()
	logs, err := io.ReadAll(reader)
	if err != nil {
		t.Logf("read %s diagnostics: %v", name, err)
	}
	directory := artifactDirectory()
	filename := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()+"_"+name+"_"+container.ID) + ".log"
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Errorf("create diagnostics directory: %v", err)
	} else if err := os.WriteFile(filepath.Join(directory, filename), logs, 0o600); err != nil {
		t.Errorf("save %s diagnostics: %v", name, err)
	}
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	if testing.Short() {
		t.Skip("real-container integration tests are disabled with -short")
	}
	prepareImages(t)
	s := &suite{client: newHTTPClient(nil)}
	t.Cleanup(s.client.CloseIdleConnections)
	var err error
	s.network, err = network.New(t.Context())
	testcontainers.CleanupNetwork(t, s.network)
	if err != nil {
		t.Fatalf("create isolated Docker network (Docker must be running): %v", err)
	}
	s.broker = runContainer(t, "broker", "apache/kafka:4.2.2",
		network.WithNetwork([]string{"broker"}, s.network),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
			"KAFKA_LISTENERS":                      "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS":           "PLAINTEXT://broker:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":      "CONTROLLER", "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@broker:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false", "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m",
		}),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list"}).WithPollInterval(time.Second).WithStartupTimeout(2*time.Minute)),
	)
	execOK(t, s.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create", "--topic", "healer-e2e-records", "--partitions", "1", "--replication-factor", "1")
	s.primary = s.startWorker(t, "connect", true)
	s.secondary = s.startWorker(t, "connect-secondary", false)
	s.healer = s.startHealer(t, "healer", applicationYAML, clustersYAML("8444"))
	s.api = endpoint(t, s.healer, "8080/tcp", "http")
	eventually(t, 30*time.Second, "authenticated Healer API ready", func() bool {
		code, _, _ := request(s.client, http.MethodGet, s.api+"/config", nil, apiUsername, apiPassword, "")
		return code == http.StatusOK
	})
	return s
}

func (s *suite) startWorker(t *testing.T, alias string, gateway bool) *worker {
	t.Helper()
	properties := fmt.Sprintf(`bootstrap.servers=broker:9092
group.id=healer-%[1]s
config.storage.topic=healer-%[1]s-configs
offset.storage.topic=healer-%[1]s-offsets
status.storage.topic=healer-%[1]s-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
offset.storage.partitions=1
status.storage.partitions=1
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
plugin.path=/opt/kafka/plugins
plugin.discovery=only_scan
listeners=http://0.0.0.0:8083,https://0.0.0.0:8443
rest.advertised.host.name=%[1]s
rest.advertised.port=8083
rest.advertised.listener=http
rest.extension.classes=org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension
listeners.https.ssl.keystore.location=/opt/kafka/e2e-tls/connect.p12
listeners.https.ssl.keystore.password=test-only-password
listeners.https.ssl.keystore.type=PKCS12
listeners.https.ssl.key.password=test-only-password
offset.flush.interval.ms=1000
`, alias)
	options := []testcontainers.ContainerCustomizer{
		network.WithNetwork([]string{alias, "wrong-host"}, s.network),
		testcontainers.WithExposedPorts("8443/tcp"),
		testcontainers.WithEnv(map[string]string{"KAFKA_OPTS": "-Djava.security.auth.login.config=/config/jaas.conf", "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"}),
		testcontainers.WithFiles(file("/config/worker.properties", properties), file("/config/credentials.properties", connectUsername+"="+connectPassword+"\n"),
			file("/config/jaas.conf", `KafkaConnect { org.apache.kafka.connect.rest.basic.auth.extension.PropertyFileLoginModule required file="/config/credentials.properties"; };`)),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Connect started").WithStartupTimeout(3 * time.Minute)),
	}
	if gateway {
		options = append(options, testcontainers.WithEntrypoint("/bin/sh", "-c"), testcontainers.WithCmd(`java -cp /opt/kafka/plugins/healer-test/healer-test.jar e2e.FaultProxy & exec /opt/kafka/bin/connect-distributed.sh /config/worker.properties`))
	}
	container := runContainer(t, alias, image("CONNECT_TEST_IMAGE", defaultConnectImage), options...)
	if len(s.ca) == 0 {
		s.ca = readFile(t, container, "/opt/kafka/e2e-tls/ca.crt")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(s.ca) {
		t.Fatal("fixture CA is not valid PEM")
	}
	w := &worker{container: container, endpoint: endpoint(t, container, "8443/tcp", "https"), client: newHTTPClient(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})}
	t.Cleanup(w.client.CloseIdleConnections)
	eventually(t, time.Minute, alias+" REST API ready", func() bool {
		code, _, _ := w.request(http.MethodGet, "/connectors", nil)
		return code == http.StatusOK
	})
	return w
}

func clustersYAML(port string) string {
	return "test:\n  host: connect\n  port: \"" + port + "\"\n  https: true\n  authConfig:\n    enabled: true\n    username: " + connectUsername + "\n    password: " + connectPassword + "\n"
}

func (s *suite) startHealer(t *testing.T, name, config, clusters string, extra ...testcontainers.ContainerCustomizer) *testcontainers.DockerContainer {
	t.Helper()
	options := []testcontainers.ContainerCustomizer{
		network.WithNetwork([]string{name}, s.network), testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithEnv(map[string]string{"SSL_CERT_FILE": "/config/ca.crt"}),
		testcontainers.WithFiles(file("/config/application.yaml", config), file("/config/clusters.yaml", clusters), file("/config/ca.crt", string(s.ca))),
		testcontainers.WithCmd("--config=/config/application.yaml", "--connect-clusters=/config/clusters.yaml"),
		testcontainers.WithWaitStrategy(wait.ForLog("starting API server").WithStartupTimeout(30 * time.Second)),
	}
	options = append(options, extra...)
	return runContainer(t, name, image("HEALER_TEST_IMAGE", defaultHealerImage), options...)
}

func endpoint(t *testing.T, container *testcontainers.DockerContainer, port, scheme string) string {
	t.Helper()
	endpoint, err := container.PortEndpoint(t.Context(), port, scheme)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func newHTTPClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsConfig}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func request(client *http.Client, method, endpoint string, body []byte, username, password, contentType string) (int, []byte, http.Header) {
	req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, []byte(err.Error()), nil
	}
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, []byte(err.Error()), nil
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, []byte(err.Error()), response.Header
	}
	return response.StatusCode, data, response.Header
}

func (w *worker) request(method, path string, body []byte) (int, []byte, http.Header) {
	return request(w.client, method, w.endpoint+path, body, connectUsername, connectPassword, "application/json")
}

func (s *suite) apiRequest(t *testing.T, method, path, body string, expected int) ([]byte, http.Header) {
	t.Helper()
	code, data, headers := request(s.client, method, s.api+path, []byte(body), apiUsername, apiPassword, "application/json")
	if code != expected {
		t.Fatalf("%s %s: got %d, want %d: %s", method, path, code, expected, data)
	}
	return data, headers
}

func (s *suite) patch(t *testing.T, patch string) {
	t.Helper()
	s.apiRequest(t, http.MethodPatch, "/config", patch, http.StatusNoContent)
}

func clusterJSON(host, port, password string) string {
	data, _ := json.Marshal(map[string]any{"host": host, "port": port, "https": true, "authConfig": map[string]any{"enabled": true, "username": connectUsername, "password": password}})
	return string(data)
}

// Restore runtime settings even after a subtest fails. Every behavioral test
// owns its connectors and uses a new name, so previous failures cannot leak in.
func (s *suite) resetAfter(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		s.setMode(t, "pass")
		s.patch(t, baselinePatch)
		code, response, _ := request(s.client, http.MethodPut, s.api+"/clusters/test", []byte(clusterJSON("connect", "8444", connectPassword)), apiUsername, apiPassword, "application/json")
		if code != http.StatusCreated && code != http.StatusNoContent {
			t.Errorf("restore primary cluster: %d %s", code, response)
		}
	})
}

func eventually(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func remains(t *testing.T, duration time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !condition() {
			t.Fatalf("%s", description)
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

func execOK(t *testing.T, container *testcontainers.DockerContainer, command ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	code, reader, err := container.Exec(ctx, command)
	if err != nil {
		t.Fatalf("exec %s: %v", command[0], err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("exec %v: exit %d, error %v: %s", command, code, err, data)
	}
	return string(data)
}

func readFile(t *testing.T, container *testcontainers.DockerContainer, path string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reader, err := container.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func putFile(t *testing.T, container *testcontainers.DockerContainer, path, data string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := container.CopyToContainer(ctx, []byte(data), path, 0o666); err != nil {
		t.Fatal(err)
	}
}

func (s *suite) setMode(t *testing.T, mode string) {
	t.Helper()
	putFile(t, s.primary.container, "/data/proxy-mode", mode)
}

type fixture struct {
	worker *worker
	name   string
	tasks  int
}

func (s *suite) connector(t *testing.T, w *worker, tasks int) *fixture {
	t.Helper()
	s.sequence++
	return s.namedConnector(t, w, "fixture-"+strconv.Itoa(s.sequence), tasks)
}

func (s *suite) namedConnector(t *testing.T, w *worker, name string, tasks int) *fixture {
	t.Helper()
	f := &fixture{worker: w, name: name, tasks: tasks}
	data, _ := json.Marshal(map[string]any{"name": name, "config": map[string]string{"connector.class": "e2e.HealerTestConnector", "tasks.max": strconv.Itoa(tasks), "test.tasks": strconv.Itoa(tasks), "test.id": name}})
	code, response, _ := w.request(http.MethodPost, "/connectors", data)
	if code != http.StatusCreated {
		t.Fatalf("create real connector: %d %s", code, response)
	}
	t.Cleanup(func() {
		code, response, _ := w.request(http.MethodDelete, "/connectors/"+url.PathEscape(name), nil)
		if code != http.StatusNoContent && code != http.StatusNotFound {
			t.Errorf("delete fixture %s: %d %s", name, code, response)
		}
	})
	f.wait(t, "RUNNING", "RUNNING")
	return f
}

func (f *fixture) marker(t *testing.T, name string, present bool) {
	t.Helper()
	path := "/data/" + f.name + "/" + name
	if present {
		putFile(t, f.worker.container, path, "fail")
	} else {
		execOK(t, f.worker.container, "rm", "-f", path)
	}
}

func (f *fixture) starts(t *testing.T, kind string) []time.Time {
	t.Helper()
	data := readFile(t, f.worker.container, "/data/"+f.name+"/"+kind+"-starts")
	var starts []time.Time
	for _, line := range strings.Fields(string(data)) {
		millis, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		starts = append(starts, time.UnixMilli(millis))
	}
	return starts
}

func (f *fixture) state(connector, task string) bool {
	code, data, _ := f.worker.request(http.MethodGet, "/connectors/"+url.PathEscape(f.name)+"/status", nil)
	if code != http.StatusOK {
		return false
	}
	var status struct {
		Connector struct{ State string }
		Tasks     []struct {
			ID    int
			State string
		}
	}
	if json.Unmarshal(data, &status) != nil || status.Connector.State != connector || len(status.Tasks) != f.tasks {
		return false
	}
	for _, state := range status.Tasks {
		if state.State != task {
			return false
		}
	}
	return true
}

func (f *fixture) wait(t *testing.T, connector, task string) {
	t.Helper()
	eventually(t, time.Minute, f.name+" "+connector+"/"+task, func() bool { return f.state(connector, task) })
}

type proxyRequest struct {
	at                 time.Time
	method, path, mode string
}

func (s *suite) requests(t *testing.T) []proxyRequest {
	t.Helper()
	data := readFile(t, s.primary.container, "/data/proxy-requests")
	var requests []proxyRequest
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 4 {
			continue
		} // An append can be observed mid-line.
		millis, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, proxyRequest{time.UnixMilli(millis), fields[1], fields[2], fields[3]})
	}
	return requests
}

func (s *suite) attempts(t *testing.T, f *fixture, mode string) []proxyRequest {
	t.Helper()
	var attempts []proxyRequest
	for _, request := range s.requests(t) {
		if request.method == http.MethodPost && strings.HasPrefix(request.path, "/connectors/"+f.name+"/") && (mode == "" || request.mode == mode) {
			attempts = append(attempts, request)
		}
	}
	return attempts
}

func logs(t *testing.T, container *testcontainers.DockerContainer) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reader, err := container.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
