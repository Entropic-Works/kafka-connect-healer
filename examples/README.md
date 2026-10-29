# Examples

Run these commands from the repository root. These examples actually restart failed
connectors/tasks when a reachable Kafka Connect cluster is configured; start against
a non-production cluster first.

## Local binary

```sh
go run ./cmd/kafka-connect-healer \
  --config=examples/application.yaml \
  --connect-clusters=examples/connect-clusters.yaml
```

This expects Kafka Connect at `localhost:8083`. To explore the API without polling:

```sh
go run ./cmd/kafka-connect-healer \
  --config=examples/application.yaml \
  --connect-clusters=examples/empty-clusters.yaml
```

The API listens on all interfaces, not only localhost. The example disables API
authentication; use it only on a trusted development machine/network. Credentials
belong in separate, untracked Secret overlay files, not these examples. See the
[configuration and authentication documentation](../README.md#configuration-api).

## Kubernetes

Edit `kubernetes/connect-clusters.yaml` to use your Kafka Connect Service hostname.
The Deployment uses `ghcr.io/entropic-works/kafka-connect-healer:0.0.4`, with
unauthenticated health probes on port 8081.

```sh
kubectl kustomize examples/kubernetes
kubectl apply -k examples/kubernetes
kubectl rollout status deployment/kafka-connect-healer
kubectl port-forward deployment/kafka-connect-healer 8080:8080 8081:8081
```

Then access the control API at `http://localhost:8080` and health probes at
`http://localhost:8081/livez` and `http://localhost:8081/readyz`.
No Service or Ingress is created. This does not
isolate the pod's unauthenticated API from other pods: use network policy and enable
API Basic Auth via a Secret overlay before sharing access. Basic Auth requires TLS
termination at a trusted proxy/Ingress when requests traverse an untrusted network.
Kafka Connect Basic Auth also supports HTTP, with an explicit warning that credentials
are sent without transport encryption. Use `tls.https: true` or protect that connection
through your deployment's network infrastructure.

For endpoints signed by a private corporate CA, follow the [corporate CA deployment
guide](../docs/CORPORATE_CA.md) to mount a trust bundle while keeping TLS verification enabled.

Kustomize generates a hashed ConfigMap name. Applying changes to either configuration
file replaces the pod; one replica and `Recreate` avoid overlapping pollers during
updates. Runtime API changes are not persisted, and backoff history resets on restart.
Resource settings are starting points, not capacity guarantees. The Deployment uses
unauthenticated HTTP probes on port 8081: `/livez` checks HTTP liveness every ten
seconds, and `/readyz` checks application readiness every five seconds. Readiness
fails while the control API on port 8080 is unavailable or when shutdown begins.
Empty cluster configurations and Kafka Connect outages
do not fail these probes; they do not measure polling or remediation success.
