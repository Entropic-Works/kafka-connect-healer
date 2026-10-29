# Kafka Connect Healer

[![CI](https://github.com/Entropic-Works/kafka-connect-healer/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Entropic-Works/kafka-connect-healer/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

An open-source project maintained by **Entropic Works, Inc.**, created by [Aaron Augustine](https://github.com/aaaugustine29).

The source repository, issue tracker, and pull requests are hosted at [Entropic-Works/kafka-connect-healer](https://github.com/Entropic-Works/kafka-connect-healer).

Polls the REST APIs of named Kafka Connect clusters and restarts failed connectors. When task restarts are enabled, it also restarts failed tasks whose connector is running. Polling begins after the first interval; the service stops on SIGINT or SIGTERM.

## Run

Requires Go 1.27.3 or later.

Clone the repository first:

```sh
git clone https://github.com/Entropic-Works/kafka-connect-healer.git
cd kafka-connect-healer
```

Run with all default values:

```sh
go run ./cmd/kafka-connect-healer
```

For a standalone binary without Docker, see [Build from source](#build-from-source).

Supply optional application settings and cluster definitions using separate YAML files:

```sh
go run ./cmd/kafka-connect-healer --config=/path/to/application.yaml --connect-clusters=/path/to/connect-clusters.yaml
```

Runnable local and Kubernetes configurations are provided in [examples/](examples/README.md). Omit `--config` to use all application defaults; an application file containing `{}` has the same effect. Omit `--connect-clusters` to poll one cluster named `default` at `http://localhost:8083`. To supply cluster definitions, use a file containing a mapping of cluster names to endpoint settings. A cluster file containing `{}` starts no pollers; it still starts the application API.

Optional Secret overlays are loaded independently:

```sh
go run ./cmd/kafka-connect-healer \
  --config=/path/to/application.yaml \
  --secret-config=/path/to/application-secret.yaml \
  --connect-clusters=/path/to/connect-clusters.yaml \
  --connect-clusters-secret-config=/path/to/connect-clusters-secret.yaml
```

Both base files and each Secret overlay are optional, but any explicitly supplied file is required. No files are discovered automatically. Unreadable or invalid files cause startup to fail before the HTTP server or polling starts; an invalid file does not fall back to defaults. An application Secret overlay can be supplied without a base file and merges onto application defaults.

The control HTTP API listens on port 8080 on all interfaces. `GET /` lists the available control routes, `GET /config` returns application settings without the API password, and `GET /clusters` returns cluster definitions without passwords. Optional API authentication protects every route on this server. A separate health HTTP server listens on port 8081 on all interfaces and serves only the unauthenticated probes below. A control API startup or serving failure triggers graceful application shutdown and exit code 1, allowing a supervisor to restart the process. A health server failure is logged as an error while polling continues. On SIGINT, SIGTERM, or control API failure, polling requests are canceled and both servers shut down concurrently with a shared five-second deadline for active requests before their connections are closed. Server logs include `component=api` or `component=health` and the listening address. Shutdown logs report the grace period, completion duration, and any failure or forced connection closure; listeners stopping are logged separately at DEBUG while active requests may still be finishing.

Kubernetes HTTP probes use port 8081 and require no authentication:

| Endpoint | Response |
| --- | --- |
| `GET /livez` | `200 OK` with `ok` while the health HTTP server responds. |
| `GET /readyz` | `200 OK` with `ok` after configuration and the cluster manager initialize and the control API listener starts; `503 Service Unavailable` with `not ready` while the control API is unavailable or when shutdown begins. |

Neither listener starts until configuration and the cluster manager initialize. If the control API fails to bind or stops serving, readiness becomes false and the application stops. If the health server fails, probes cannot connect. Readiness becomes false as soon as SIGINT, SIGTERM, or control API failure cancels the application context, before waiting for pollers to stop. Both probes work with an empty cluster configuration and remain healthy during Kafka Connect outages or connector failures: they describe the healer's lifecycle, not successful polling or remediation. Responses use `Cache-Control: no-store` and expose no configuration or credentials. Expose port 8080 for control access as needed and restrict port 8081 to probe traffic separately. The [Kubernetes example](examples/kubernetes/deployment.yaml) configures [liveness and readiness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/).

The control API has a five-second header timeout, a five-second timeout for reading the entire request including its body, and a ten-second response-write timeout. The health server has a one-second header timeout and two-second request-read and response-write timeouts, leaving room within the five-second shutdown deadline for incomplete probe connections to close. Both servers have a one-minute idle timeout. All polling, restart, and backoff logs include `connect_cluster` and `endpoint`; action logs also identify the connector and, for task restarts, `task_id`. Startup logs report the configured cluster count and source, and each poller logs its effective settings, changes, and shutdown at INFO. Restart logs report requests as accepted, since an HTTP success response does not prove that the connector or task has finished restarting. Repeated failure detection, backoff skips, and poll-cycle counts and durations are logged at DEBUG; request attempts are logged at INFO and failures at ERROR. Authentication credentials are omitted. Normal shutdown cancellation does not generate polling failure logs.

Each action event includes `action=restart_connector` or `action=restart_task`.
Each request has a start event and a single outcome event. Accepted outcomes are
INFO, HTTP 409 conflicts are WARN, and other failures are ERROR. Outcome events
include `duration`, `request_attempted`, and `status_code`; attempted requests
also include `attempt_count` and `attempted_at`, including unsuccessful requests. A status code
of `0` means no HTTP response was received. Connector restart events omit
`task_id`; they do not invent a task ID of zero. DEBUG determination and backoff
events retain the same action identity, so concurrent cluster logs can be traced
without relying on their ordering.

INFO `connector recovered` and `task recovered` events are emitted once when a
previously observed failing target returns to RUNNING, even if Healer did not
restart it. Recovery history survives status-fetch errors, but deleted targets
are forgotten. These events report observed recovery, not its cause. Normal
shutdown request cancellation is DEBUG rather than a failure.

## Build from source

Build a standalone binary with Go, or build a container image with the project's
Dockerfile.

Clone the repository as shown above and check out the release tag or commit you
have reviewed. Use Go 1.27.3 or later, as required by `go.mod`. For consistent builds,
use the same Go version across your build machines.

Build for the current machine (commands below use a POSIX shell):

```sh
GOTOOLCHAIN=local go mod download
go mod verify
CGO_ENABLED=0 GOTOOLCHAIN=local \
  go build -mod=readonly -trimpath -o bin/kafka-connect-healer ./cmd/kafka-connect-healer
```

Dependencies are downloaded into Go's module cache using the versions and
checksums recorded in `go.mod` and `go.sum`. A fresh build needs network access to
populate that cache. No C compiler is required. `-mod=readonly` prevents builds
from silently updating dependency requirements. `GOTOOLCHAIN=local` makes the
build fail if the installed Go version is too old rather than automatically
downloading a newer one.

Run the binary with the example configuration:

```sh
./bin/kafka-connect-healer \
  --config=examples/application.yaml \
  --connect-clusters=examples/connect-clusters.yaml
```

The cluster example points to `localhost:8083`; change it for your environment, or
use `examples/empty-clusters.yaml` to explore the API without polling. The example
disables API authentication, and the API listens on all interfaces. Restrict network
access and configure authentication before exposing it.

To cross-compile a Linux amd64 binary, even from macOS:

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOTOOLCHAIN=local \
  go build -mod=readonly -trimpath -o bin/kafka-connect-healer-linux-amd64 ./cmd/kafka-connect-healer
```

Use `GOARCH=arm64` and a corresponding output filename for Linux arm64. The Linux
binary is statically linked and does not need Go installed at runtime. HTTPS requests
still require trusted CA certificates on the target system; include your organization's
trusted certificates when using an internal image. Run as an unprivileged user and
make configuration files readable by that user. Secret overlay flags work the same
way as when using `go run` or Docker.

For internal redistribution, include `LICENSE`, `NOTICE`, and applicable third-party
license notices alongside your binary or image. Keep the reviewed source revision
and Go version in your build records. To collect dependency notices into a new
directory, run the following from the repository root, with the same `GOOS` and
`GOARCH` used for your binary:

```sh
sh scripts/collect-licenses.sh bin/dependency-licenses
```

See [Tests](#tests) for pre-deployment checks.

## Docker

Release images are published to `ghcr.io/entropic-works/kafka-connect-healer` on GitHub Container Registry (GHCR)
for Linux amd64 and arm64. Use an explicit published version tag, such as `0.0.4`.

Build the image:

```sh
docker build -t kafka-connect-healer:local .
```

The build uses Go 1.27.3 and downloads and verifies dependencies in a separate
Docker layer. Changes to application code reuse that layer; changes to `go.mod`
or `go.sum` refresh it. Compilation and license collection then run with networking
disabled. The final `scratch` image contains only the stripped, statically linked
binary, public HTTPS CA certificates, and license notices. It runs as non-root
UID/GID `65532`, contains no shell or package manager, and supports a read-only
root filesystem. Build tools and dependency source code stay in the build stage.
`.dockerignore` limits the build context to application source, module files, the
license collector, and project notices. Mount configuration and secrets at runtime.

To run with YAML files in an existing local `config` directory:

```sh
docker run --rm --read-only -p 127.0.0.1:8080:8080 \
  --mount type=bind,src="$PWD/config",dst=/config,readonly \
  kafka-connect-healer:local \
  --config=/config/application.yaml \
  --connect-clusters=/config/connect-clusters.yaml
```

Add `-p 127.0.0.1:8081:8081` to access the health endpoints from the host. Mounted files must be readable by UID `65532`. Supply Secret overlay flags the same way when needed. Inside the container, `localhost` refers to the container itself; configure a reachable Kafka Connect hostname instead of relying on the default localhost endpoint. For Kubernetes, use the mounted file paths described below.

Build for another architecture with `docker buildx build --platform=linux/amd64 --load -t kafka-connect-healer:local .`; the Dockerfile cross-compiles for the selected target platform.

## Configuration

Both configuration packages read their own files once at startup, in this order:

1. Application defaults; for clusters, a single `default` cluster when no base file is supplied, or endpoint defaults for each named cluster in a supplied file.
2. Its optional base YAML file, normally mounted from a Kubernetes ConfigMap.
3. Its optional Secret YAML overlay, normally mounted from a Kubernetes Secret.

Application settings live in `internal/config`; cluster configuration lives in `internal/connectcluster`. The files use independent schemas. Application settings remain at the root of the application file, with sections such as `pollingBehavior` and `apiConfig`. The cluster file contains named entries:

```yaml
production:
  host: connect.production.svc
  port: "8083"
  tls:
    https: true
    insecureSkipVerify: false
  authConfig:
    enabled: true
staging:
  host: connect.staging.svc
```

The map key identifies the cluster; there is no separate `name` field. When a cluster base file is supplied, only its entries and those added by its overlay are created. Each new cluster starts with endpoint defaults, so the staging entry above retains port `8083` and disables HTTPS and authentication.

Each supplied field replaces its previous value, including `false` and empty strings. Omitted fields retain their previous values; an empty mapping (`{}`) changes nothing. A supplied nested mapping changes only its supplied fields. Cluster overlays merge each named entry onto that cluster's existing values, preserving its endpoint and omitted credentials. An overlay may add a named cluster or change any field. Without a cluster base file, the overlay merges onto the initial `default` cluster and may add further clusters.

Each merged configuration is validated after its base file and overlay have been loaded, so the base can enable authentication and leave credentials for the overlay. Keep actual credentials outside version control and out of ConfigMaps. A cluster Secret overlay can supply credentials as follows:

```yaml
production:
  authConfig:
    username: replace-me
    password: replace-me
```

Cluster Basic Auth requires a nonblank username without a colon and a nonempty password. Invalid authentication settings fail startup; authentication is never silently disabled. HTTP is allowed with authentication enabled, but credentials are sent without transport encryption. The application logs an explicit warning with the cluster name and endpoint when such a cluster starts or its definition changes; credentials are never included in the warning. Use `tls.https: true` or protect the connection through your deployment's network infrastructure. All clusters share the application's polling behavior, request timeout, and logging policy, while maintaining independent restart backoff history. Each cluster reuses one HTTP client for status and restart requests; timeout updates apply between polling cycles without replacing that client.

TLS settings belong to each cluster's `tls` mapping. With `tls.https: true`, certificate-chain and hostname verification are enabled by default using the system trust store. Setting `tls.insecureSkipVerify: true` keeps HTTPS encryption but disables both checks, leaving the server's identity unverified. The application logs an explicit warning with the cluster name and endpoint once when that definition is applied. `insecureSkipVerify: true` requires `tls.https: true`; otherwise startup or the cluster update is rejected. Each cluster owns its transport and TLS policy, and stopping or replacing its poller closes its idle connections.

PUT replaces the complete cluster definition, so omitting `tls.insecureSkipVerify` restores certificate verification.

Clusters configured with the same literal endpoint generate a startup warning but remain enabled. The comparison accounts for hostname casing, a trailing DNS dot, equivalent IPv6 spellings, and leading zeros in ports; it does not resolve DNS aliases or discover whether different endpoints belong to the same Kafka Connect cluster. Duplicate pollers maintain separate backoff histories and may issue duplicate restart requests. Warnings never include authentication credentials.

The available settings and defaults are:

| YAML field | Default |
| --- | --- |
| `<cluster>.host` | `localhost` |
| `<cluster>.port` | `"8083"` |
| `<cluster>.tls.https` | `false` |
| `<cluster>.tls.insecureSkipVerify` | `false` |
| `<cluster>.authConfig.enabled` | `false` |
| `<cluster>.authConfig.username` | empty string |
| `<cluster>.authConfig.password` | empty string |
| `apiConfig.authConfig.enabled` | `false` |
| `apiConfig.authConfig.username` | empty string |
| `apiConfig.authConfig.password` | empty string |
| `communicationConfig.requestTimeout` | `10s` |
| `pollingBehavior.interval` | `10s` |
| `pollingBehavior.restartFailedTasks` | `true` |
| `pollingBehavior.backoff.enabled` | `true` |
| `pollingBehavior.backoff.baseDelay` | `20s` |
| `pollingBehavior.backoff.maxDelay` | `10m` |
| `pollingBehavior.backoff.exponential` | `true` |
| `loggingConfig.level` | `INFO` |
| `telemetryConfig.enabled` | `false` |

In the cluster file, `<cluster>.host` is a DNS hostname or unbracketed IP address (for example, `kafka-connect`, `127.0.0.1`, or `2001:db8::1`), without a URL scheme, port, path, or whitespace. Invalid host syntax is rejected at startup. `<cluster>.port` is a string containing only decimal digits, between 1 and 65535, such as `"8083"`. In the application file, `loggingConfig.level` accepts Go slog levels such as `DEBUG`, `INFO`, `WARN`, and `ERROR`. Connect requests do not follow redirects; a redirect is treated as an unsuccessful status rather than allowing a restart POST to become a GET or send credentials to another endpoint.

Durations are positive strings with units, such as `250ms`, `10s`, `1.5s`, or `1m30s`. Supported units are `ns`, `us` (or `µs`), `ms`, `s`, `m`, and `h`. Numeric, invalid, nonpositive, or overflowing durations are rejected. The maximum backoff delay must be at least the base delay. The default base delay is `20s`, independently of any configured polling interval.

Each file must contain exactly one YAML mapping document. Unknown keys, duplicate keys, null values, aliases, and YAML merge keys are rejected. An application file or Secret overlay containing `{}` keeps existing values; a cluster base file containing `{}` starts with no clusters. Decoder errors omit raw values to prevent credentials from reaching logs.

## Kubernetes

Mount any application and cluster base files from a ConfigMap and their optional overlays from a Secret, using read-only directory mounts. A single ConfigMap can contain both base files, and a single Secret can contain both overlays. Pass the file paths through the corresponding command-line flags. The image entrypoint must launch the application binary. If defaults suffice, omit the corresponding file flags and mounts.

For Kustomize deployments, generate the ConfigMap and Secret from these YAML files and keep generated name hashes enabled. Changes to input files generate a new resource name and update the Deployment's volume reference when applied, triggering replacement of the pod. This makes configuration and credential changes follow the same deployment process. See [Kustomize generators](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/).

Use one replica and the `Recreate` strategy to avoid overlapping pollers issuing duplicate restart requests during updates. Updates include a brief polling interruption and reset the application's in-memory restart backoff history.

For Helm deployments, mount the same files and use pod-template checksum annotations for both chart-managed ConfigMaps and Secrets. Independently managed Secret changes also need an explicit pod restart or a deployment controller that triggers one. See [Helm's rollout guidance](https://helm.sh/docs/howto/charts_tips_and_tricks/#automatically-roll-deployments).

Configuration is only read at startup. Updating a mounted file does not change the running application's configuration; restart the pod to apply it. Mounting files does not require the application to access the Kubernetes API. See [ConfigMaps](https://kubernetes.io/docs/concepts/configuration/configmap/) and [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/).

## Restart backoff

Backoff is tracked separately for each cluster, connector restart, and task restart. Identical connector names in different clusters do not share backoff state. With exponential backoff enabled, the delay after attempt number `n` is `min(base delay × 2^(n−1), maximum delay)`. For example, the default delays begin at 20 seconds, then 40 seconds, 80 seconds, and so on up to 10 minutes. When exponential backoff is disabled, each attempt uses the base delay. Failed connectors are restarted regardless of `pollingBehavior.restartFailedTasks`; that setting controls failed tasks whose connector is running.

Every outbound restart request counts as an attempt, including requests that return errors. A `RUNNING` connector clears its connector backoff, and a `RUNNING` task clears that task's backoff; a future failure then starts again at the base delay. A successful status snapshot also removes backoff entries for connectors and tasks that are no longer present. If status retrieval or decoding fails, no actions, resets, or cleanup happen on that poll.

## Configuration API

API Basic Auth is disabled by default and is independent of Kafka Connect authentication. To enable it, put this in the base configuration:

```yaml
apiConfig:
  authConfig:
    enabled: true
```

Supply credentials in the Secret YAML overlay, not the ConfigMap:

```yaml
apiConfig:
  authConfig:
    username: healer-admin
    password: replace-me
```

Enabled API authentication requires a nonblank username without a colon and a nonempty password; otherwise startup fails. Missing, malformed, or incorrect credentials return `401 Unauthorized` with a Basic Auth challenge before any control route handler runs. All routes on port 8080 require the same credentials; the health server on port 8081 remains unauthenticated. Clients can use `curl --user healer-admin https://your-healer-host/config` to be prompted for the password. Credentials are never included in authentication error responses or logs.

API authentication is startup-only: PATCH cannot enable, disable, or change its credentials. Update the startup files and restart to apply those changes. Resubmitting identical API settings is a no-op and is allowed.

Basic Auth does not encrypt credentials. The service still listens using HTTP; use a trusted TLS-terminating ingress/proxy and restrict direct backend access. Protect the proxy-to-service connection as appropriate for your deployment, and do not expose the unauthenticated default API to untrusted networks. See [Go's Basic Auth documentation](https://pkg.go.dev/net/http#Request.SetBasicAuth).

`GET /config` returns only the effective application configuration as JSON, omits the API authentication password, and sets `Cache-Control: no-store` to prevent caching. Durations use the same string format as YAML, with Go's canonical spelling (for example, `10m` is written as `10m0s`). Numeric JSON durations are rejected.

`PATCH /config` accepts a partial application JSON object using the same field names and requires `Content-Type: application/json` (parameters such as `charset=utf-8` are accepted). Missing, malformed, or unsupported content types return `415` with `Accept-Patch: application/json`. Supplied fields replace their current values; omitted fields retain them, including an omitted API authentication password. The complete resulting configuration must pass the same validation as startup. Invalid updates return `400` without changing stored values. Field errors identify the affected path without exposing submitted values. Successful updates return `204 No Content` and notify the polling and logging workers. Bodies larger than 64 KiB return `413`; body-read timeouts return `408`.

Telemetry enablement is startup-only. Changing `telemetryConfig.enabled` returns
`400` with `telemetryConfig: telemetry is startup-only; update the YAML configuration and restart`.
The entire patch is rejected, including any other supplied changes. Omitting
telemetry settings or resubmitting their current value is allowed; a patch with
no effective changes returns `204` without notifying workers. `GET /config`
includes the current telemetry setting. OTLP endpoint and authentication settings
use environment variables and are not accepted by this API.

```sh
curl -X PATCH http://localhost:8080/config \
  -H 'Content-Type: application/json' \
  -d '{"pollingBehavior":{"interval":"30s","restartFailedTasks":false}}'
```

JSON handling uses Go's `encoding/json/v2`. Null values and duplicate keys are rejected at every nesting level. Duplicate checking also catches escaped spellings and casing differences such as `interval` and `Interval`. Repeating a key in separate objects is allowed. Unknown fields and invalid UTF-8 are rejected too. Cluster configuration fields are unknown to this endpoint and return `400`. JSON decoding errors use JSON Pointer paths, such as `/pollingBehavior/interval`; configuration validation errors use dotted paths.

Application updates affect only the running process. YAML files are never modified, and restarting restores configuration from those files.

### Cluster management

`GET /clusters` returns a JSON object keyed by cluster name, omits passwords, and sets `Cache-Control: no-store`. An empty collection is returned as `{}`.

`PUT /clusters/{name}` creates or replaces one complete cluster definition; it does not merge with the existing definition or apply YAML defaults. Host and port are required, and enabled authentication requires supplied credentials. Authenticated HTTP definitions are accepted with the same transport encryption warning described above. Omitted fields become their zero values, including an omitted password; there is no password-preservation behavior. Invalid definitions leave the current poller untouched. PUT requires `Content-Type: application/json` and rejects unknown fields, duplicate keys, null values, invalid UTF-8, and malformed JSON. Bodies larger than 64 KiB return `413`, unsupported content types return `415`, and body-read timeouts return `408`.

Creating a cluster returns `201 Created` with a `Location` header. Replacing a cluster returns `204 No Content`: the old poller is canceled and awaited before its replacement starts with a new client and fresh backoff history. An identical definition returns `204` without replacing the poller or resetting its history. Other clusters are unchanged. Duplicate endpoints produce a warning but are allowed.

```sh
curl -X PUT http://localhost:8080/clusters/production \
  -H 'Content-Type: application/json' \
  -d '{"host":"connect.production.svc","port":"8083","tls":{"https":false},"authConfig":{"enabled":false}}'
```

`DELETE /clusters/{name}` cancels that cluster's poller, waits for it to exit, and removes its definition before returning `204`. An unknown name returns `404`. Removal stops future polling but cannot undo restart requests already accepted by Kafka Connect. PUT and DELETE return `503` when the application is shutting down. New pollers use the application's lifetime context, so completing an API request does not stop them.

Cluster updates are also in-memory only. Startup YAML files remain untouched, and restarting restores the definitions from those files. API authentication settings remain startup-only.

## License

Copyright 2026 Entropic Works, Inc. See [NOTICE](NOTICE) for attribution.

Kafka Connect Healer is licensed under the [Apache License, Version 2.0](LICENSE).
Third-party dependencies retain their own licenses and notices. The Docker build
collects them from the module cache for the modules imported by the application,
preserving their paths under `/licenses/dependencies/`. That directory also
contains `modules.txt`, which records their module paths and versions. The
collector discovers dependencies automatically when imports change.

## Dependencies

Dependencies use Go modules; their source is cached outside the repository.
Commit `go.mod` and `go.sum` after adding, removing, or updating a dependency:

```sh
go get example.com/module@v1.2.3
go mod tidy
go mod verify
```

Review the module-file changes and run the checks below. CI downloads dependencies,
verifies the module cache, checks `go mod tidy -diff`, and runs build/test commands
with `GOFLAGS=-mod=readonly`. Root and integration dependencies remain in separate
modules. Apply dependency changes and run `go mod tidy` in the module that uses
them. See [Go's module documentation](https://go.dev/ref/mod).

## Tests

```sh
go test ./...
```

Run the same build, static analysis, and race-enabled tests used by CI:

```sh
go build ./...
go vet ./...
go test -race -shuffle=on -count=3 -timeout=5m ./...
```

CI also checks formatting, verifies dependency checksums, runs `govulncheck`, builds
and smoke-tests the Docker image, renders the Kubernetes example, and runs the
Testcontainers integration suite against real Kafka and Connect. The smoke test
checks API reads and updates, cluster creation/deletion, health probes, authentication, password
redaction, packaged attribution, and clean shutdown with a non-root, read-only
container. It does not test remediation against a real Kafka Connect cluster.
Go is selected from `go.mod`.

Run the additional checks locally:

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
docker build -t kafka-connect-healer:local .
bash scripts/docker-smoke-test.sh kafka-connect-healer:local
kubectl kustomize examples/kubernetes
```

Run the real Kafka/Connect integration tests with Docker running (no Kubernetes,
kubectl, or existing Kafka deployment required):

```sh
go -C tests/integration test -race -count=1 -timeout=20m ./...
```

The Go test setup builds current local test images automatically, then runs the
suite with race detection. Build and container logs are saved in
`tests/integration/artifacts/`. The suite has a separate Go
module, so `go test ./...` at the project root remains the fast unit-test suite
and Testcontainers stays outside the application's dependency graph.

## Telemetry

Set `telemetryConfig.enabled: true` in the application YAML to export metrics and
all application logs. Polling metrics cover every Connect cluster, including
clusters added through the API. This setting is startup-only. Console logging
remains enabled, and both outputs follow `loggingConfig.level`. Log export starts
after application configuration is loaded and telemetry initializes; earlier
configuration failures remain console-only. SDK/exporter diagnostics also stay
console-only to avoid exporting errors about failed log exports.

Export uses OTLP/HTTP with protobuf. Configure the shared destination with the
standard exporter environment variables, for example:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=https://telemetry.example.com
export OTEL_EXPORTER_OTLP_HEADERS='Authorization=Bearer%20<token>'
```

The exporters append `/v1/metrics` and `/v1/logs` to the shared endpoint. Platforms
with separate signal URLs can use `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` and
`OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` instead; these must include the complete paths.
Authentication headers and TLS certificates use the standard `OTEL_EXPORTER_OTLP_*`
settings and are not part of the configuration API. HTTP endpoints are supported
for local collectors. The service name defaults to `kafka-connect-healer` and can
be overridden with `OTEL_SERVICE_NAME`.

Every metric has a `connect_cluster` attribute:

| Metric | Type | Additional attributes |
| --- | --- | --- |
| `healer.poll.cycles` | Counter | `outcome=success\|error\|canceled` |
| `healer.poll.duration` | Histogram, seconds | `outcome` |
| `healer.restart.requests` | Counter | `connector`, `action`, `task_id` (task restarts only), `outcome=accepted\|error\|canceled` |

A cycle succeeds when status retrieval and all attempted restarts complete without
error. Failed connectors and backoff skips do not themselves make a cycle fail.
Restart counters count attempted HTTP requests; an accepted request does not prove
recovery. `action` is `restart_connector` or `restart_task`; task restarts include
the numeric `task_id`, including task zero. Cycle counts and duration describe
the whole cluster and have no connector or task attributes. Normal shutdown and
cluster-replacement cancellation are separate from errors, while request timeouts
count as errors.

Counters retain process-lifetime history for each cluster name. Cycle rates,
error counts, and missing-data alerts can be used to monitor polling activity.

Metrics export periodically and logs export in batches using bounded buffering.
Startup does not check destination connectivity or credentials. With no endpoint
configured, the pinned exporters default to `https://localhost:4318`. Invalid
exporter environment values may produce a console diagnostic and fall back to SDK
defaults. An initialization error returned to the application (for example,
malformed `OTEL_RESOURCE_ATTRIBUTES`) stops startup with exit code 1.

An unreachable destination, rejected credentials, or TLS errors during export
produce console diagnostics without stopping polling or the HTTP servers.
Telemetry delivery is best effort: failed batches or a full log queue can lose
records. Shutdown attempts to flush telemetry within five seconds after the
pollers and HTTP servers stop.

## Contributing and security

A real Kafka/Connect integration stack and repeatable failure tests are available
in [tests/integration/](tests/integration/README.md), using Testcontainers. It uses Apache
Kafka 4.2.2, Kafka Connect 4.3.1, and test-only connectors; it does not establish compatibility
with every Kafka version or production connector plugin.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and pull request guidance,
and [SECURITY.md](SECURITY.md) for private vulnerability reporting and deployment
precautions.

Contributions target `dev`; maintainers promote reviewed changes through `staging`
and `main`, the default and release branch. See [MAINTAINERS.md](MAINTAINERS.md), the
[code of conduct](CODE_OF_CONDUCT.md), and [repository workflow](docs/REPOSITORY.md).

## Releases

The release workflow publishes to GHCR's `ghcr.io/entropic-works/kafka-connect-healer`
repository only after the full CI checks pass for the tagged commit. It builds Linux
amd64 and arm64 images and attaches an SBOM and build provenance. Only the full
version tag is published (`0.1.0`, or a prerelease such as `0.1.0-rc.1`); no floating
`latest`, major, or minor tags are updated. The runtime image includes `LICENSE`,
`NOTICE`, and dependency licenses.

Release images support anonymous pulls from GHCR. Maintainers can find publication
instructions in the [release runbook](docs/RELEASING.md#tag-and-publish).

Stable release tags must point to a commit promoted to `main`; prerelease
tags must point to a commit promoted to `staging`. Both rerun the full CI suite,
including integration tests, before publishing. Follow the
[release runbook](docs/RELEASING.md) for promotion, tagging, publishing credentials,
and release notes. See [repository workflow](docs/REPOSITORY.md) for branch
and tag protections. After full CI, the workflow publishes images and smoke tests
both platform digests on native runners before creating a GitHub release with
verified digests. It does not upload standalone binary archives.
