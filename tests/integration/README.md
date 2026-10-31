# Testcontainers integration tests

Black-box tests run the real packaged Healer, an Apache Kafka 4.2.2 KRaft broker,
and two independently configured Kafka Connect 4.3.1 workers, alongside an official
OpenTelemetry Collector 0.163.1 pinned by image index digest. A test-only source
connector produces actual Kafka records and can deliberately fail its connector
or individual tasks. An HTTPS fault gateway injects responses and records every
request without recording credentials.

The fixture build uses JDK 25 with `javac --release 21`, matching the Java 21
runtime in the Kafka Connect image. Compiler updates must preserve that target
unless the runtime is also upgraded; otherwise the connector and fault gateway
cannot load.

## Running

Install the Go version from the root `go.mod` and Docker with its CLI and BuildKit. Docker Desktop
(including Apple Silicon) or a Linux Docker daemon works; Kubernetes is not needed.
Allow approximately 4 GB of Docker memory for this suite, plus memory for any other
running workloads. The first run needs network access to pull public images and
download the separate test module. No credentials or repository secrets are needed.

From the project root:

```sh
go -C tests/integration test -race -count=1 -timeout=20m ./...
# Build the same images automatically, but run only a selected group:
go -C tests/integration test -race -count=1 -timeout=20m -run 'TestIntegration/API' ./...
```

Alternatively, from the test module directory (images are still built automatically):

```sh
cd tests/integration
go test -race -count=1 -timeout=20m -v ./...
go test -race -count=2 -timeout=30m -v -run 'TestIntegration/RequestFailures' ./...
go vet ./...
```

The setup builds both images once per test process, even with `-count=2`, using
Docker's build cache. It checks the daemon with a 15-second timeout and allows up
to five minutes per image build. Build output is saved in `docker-info.log`,
`build-healer.log`, and `build-connect.log`; failures also include command output
in the test result. Bash is not needed. The application build still uses the real
production Dockerfile and its BuildKit cache mounts.

`HEALER_TEST_IMAGE` and `CONNECT_TEST_IMAGE` override the local destination tags;
they are always rebuilt, not treated as prebuilt inputs. Tests never push images.
`INTEGRATION_ARTIFACTS` chooses the directory for build and container logs. The
default is `artifacts/` inside this module, ignored by Git. CI uses the same Go
entrypoint and preserves JSON test results and logs as an artifact for seven days.
Collector output is also saved as newline-delimited OTLP JSON for metrics and logs.
The primary Healer runs with telemetry enabled; its test collector receives
OTLP/HTTP on the isolated Docker network and writes only to its own container filesystem.
This needs no external observability platform or credentials.

Always use `-count=1` (or another explicit count) for a real integration run. Go's
test-result cache does not reliably track source files in the parent module or
Docker state, so a cached successful result must not substitute for a fresh run.
`-short`, `-list`, and runs selecting only setup unit tests do not build images,
check Docker, or create containers:

```sh
go -C tests/integration test -short ./...
go -C tests/integration test -list . ./...
```

The suite creates a fresh, uniquely named Docker network and containers with
random host ports. It copies configuration into its containers instead of using
host mounts, so it does not modify local configuration or Kubernetes resources.
Testcontainers cleanup runs after tests (including startup failures), with Ryuk
as a fallback if the test process dies. Docker being unavailable is a failure,
not a silent skip. `-short` explicitly skips the real-container suite.

Subtests run sequentially to share the expensive infrastructure; each behavior
owns its connectors and restores runtime settings in cleanup. Do not add
`t.Parallel()` to these shared-fixture subtests. Filtered runs still create their
own full stack. No scenario requires another scenario to have run first.

## Coverage

| Group | Behaviors verified |
| --- | --- |
| API | Separate unauthenticated health listener, control/health route isolation, all-control-route Basic authentication, challenges, denied writes, routing/method status codes, password redaction, no-store headers, JSON duration strings, atomic partial PATCH, duplicate/unknown/null/invalid UTF-8/trailing JSON rejection, validation, 64 KiB limits, media types, slow-body timeouts, escaped cluster names and Location |
| HTTPAuthentication | Authenticated HTTP startup with a Secret overlay, real task and connector restarts and observed recovery, rejected credentials and unauthenticated requests, invalid/identical PUTs preserve the worker, HTTP/HTTPS configuration changes, one credential-free warning per applied authenticated HTTP definition, no warning for authenticated HTTPS or unauthenticated HTTP |
| TLSVerification | Trusted certificates verified by default; explicit bypass for untrusted certificates and hostname mismatches at startup through a Secret overlay and through PUT; real task/connector restarts and observed recovery; per-cluster isolation; restoring verification by omitting the bypass flag; invalid/identical updates preserve the worker; warning deduplication and credential redaction |
| KafkaAndTLS | Real plugin loading, trusted HTTPS, outbound Basic authentication, running tasks, actual source records consumed from Kafka |
| TaskPolicyBackoffAndRecovery | Disabled/enabled task policy, real task restarts, exponential/capped backoff, healthy reset, accepted-attempt and recovery logging |
| ConnectorFailureIgnoresTaskPolicy | Failed connectors restart even when task policy is disabled; connector restart query parameters and recovery |
| PausedAndStoppedConnectors | Intentional pause/stop is not remediated |
| OnlyFailedTaskIsRestarted | Failed task 1 restarts while tasks 0 and 2 remain untouched |
| FixedAndDisabledBackoff | Fixed delay, disabled throttling, turning task policy off at runtime |
| RuntimeIntervalAndLogLevel | Live polling interval and log-level updates |
| ClusterLifecycle | DELETE stops work, PUT creates/replaces workers, identical PUT avoids restarting a worker, credential replacement |
| RequestFailures | Restart 401/409/500/302/disconnect/timeout all count as attempts and receive backoff; no redirect following; correlated action/cluster/connector/task/status/count/timing log fields and correct severity |
| InvalidSnapshots | Malformed/null/incomplete/500/redirect status responses cause no remediation and preserve attempt history; a valid empty snapshot prunes it |
| TimeoutTLSAndClusterIsolation | Runtime request timeout, responsive API during slow outbound requests, hostname verification, identical connector names isolated across clusters, independent poller progress, deletion cancels an in-flight request |
| BrokerOutage | API/config remain usable while the broker is stopped; a restarted broker resumes real record production |
| ConfigurationLogging | Independent application and cluster summaries after successful API changes; full cluster lists after add, replace, and delete; rejected and unchanged requests stay quiet; credential redaction; immediate INFO/WARN filtering |
| Telemetry | Received OTLP metrics and logs from the packaged application; service identity; cycle counts and duration per cluster; accepted/error restart requests per cluster, connector, and task; same connector name across clusters; API/health/polling logs; collector outage leaves polling, control API, and health probes working; export resumes after collector restart; disabled telemetry stays silent; shutdown flushes logs before a long batch interval expires |
| StartupAndShutdown | Defaults, API-only/omitted cluster configuration, application and cluster Secret overlays, invalid/missing YAML startup failure, untrusted Connect certificate rejection, control API bind failure stops polling and exits 1, SIGTERM during an outbound request, clean shutdown, runtime changes lost on restart |

Unit tests remain responsible for exact backoff arithmetic/overflow, exhaustive
status decoding and configuration combinations, and data-race-sensitive internal
manager behavior. Container tests assert observable behavior and minimum retry
delays with bounded readiness waits; they do not replace those precise unit tests.

## Scope and safety

All passwords and the certificate baked into `testdata/Dockerfile.connect` are
deliberately public **test fixtures**. Never deploy this fixture image in production.
Its 30-day self-signed certificate is regenerated when its image is rebuilt;
rebuild without cache if an old local fixture image's certificate has expired.
Tests normally trust this certificate explicitly. `TLSVerification` deliberately removes trust or uses a hostname absent from the certificate to check rejection and explicit verification bypass; its strict control cluster must continue rejecting the certificate.

The suite uses one broker and one worker per independent cluster. It does not
claim coverage of multi-broker durability, same-cluster multi-worker rebalances,
Kafka SASL, ingress TLS, every Kafka version, or production connector plugins.
HTTP 409 is injected deterministically rather than relying on rebalance timing.
The broker outage preserves its container data; it is not a lost-data disaster
recovery test. Kubernetes manifests are still rendered separately in CI, but
these tests intentionally do not test Kubernetes scheduling or ConfigMap mounts.

Startup checks cover indented default and Secret-overlay summaries, empty cluster lists, and credential redaction. Telemetry checks verify that startup summaries also reach the Collector as structured records.
