# Corporate CA trust

Kafka Connect Healer uses the system trust store for HTTPS. In Linux containers,
mount a PEM trust bundle and set `SSL_CERT_FILE` to its path. This configures trust
for every Connect cluster in that process; it is not a per-cluster setting.
See [Go's system certificate pool documentation](https://pkg.go.dev/crypto/x509#SystemCertPool).

Keep these settings in each cluster definition:

```yaml
tls:
  https: true
  insecureSkipVerify: false
```

The configured Connect hostname must match the server certificate. Adding a CA
does not resolve a hostname mismatch. The server should present its required
intermediate certificates during the TLS handshake.

## Prepare a trust bundle

Obtain your corporate CA certificates in PEM format. Combine them with the public
roots from the application image so public HTTPS destinations remain trusted.
Run these commands from the repository root, using the same image you will deploy:

```sh
# Build this checkout's image, or set HEALER_IMAGE to your chosen release image.
docker build -t kafka-connect-healer:local .
HEALER_IMAGE=kafka-connect-healer:local
mkdir -p config/trust
trust_container_id=$(docker create "$HEALER_IMAGE")
docker cp "$trust_container_id:/etc/ssl/certs/ca-certificates.crt" config/trust/public-ca-bundle.pem
docker rm "$trust_container_id"
cat config/trust/public-ca-bundle.pem /path/to/corporate-ca.pem > config/trust/ca-bundle.pem
chmod 0644 config/trust/ca-bundle.pem
```

`docker create` does not start the application. The image uses `scratch` and has
no shell, so bundle assembly happens on the host. The `config/` directory is
ignored by Git. Ensure the bundle and its parent directories are readable and
traversable by the container's UID/GID `65532`.

## Docker

With application and cluster YAML files already in `config/`, mount that directory
read-only and point `SSL_CERT_FILE` to the combined bundle inside the container:

```sh
docker run --rm --read-only -p 127.0.0.1:8080:8080 \
  --mount "type=bind,src=$(pwd)/config,dst=/config,readonly" \
  --env SSL_CERT_FILE=/config/trust/ca-bundle.pem \
  "$HEALER_IMAGE" \
  --config=/config/application.yaml \
  --connect-clusters=/config/connect-clusters.yaml
```

For a native Linux binary, set `SSL_CERT_FILE` to the bundle's host path in the
process environment before starting the binary. Installing the corporate CA into
the host's system trust store is also supported.

## Kubernetes

Use a ConfigMap for the public CA certificates and mount it read-only. The following
Kustomize overlay extends [the Kubernetes example](../examples/kubernetes).
Configure its Connect endpoints and image for your deployment as described in
[the examples guide](../examples/README.md#kubernetes).

Prepare a local overlay directory:

```sh
mkdir -p config/kubernetes-corporate-ca
cp config/trust/ca-bundle.pem config/kubernetes-corporate-ca/ca-bundle.pem
```

Create `config/kubernetes-corporate-ca/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../examples/kubernetes
configMapGenerator:
  - name: kafka-connect-healer-trust
    files:
      - ca-bundle.pem
patches:
  - path: trust.yaml
```

Create `config/kubernetes-corporate-ca/trust.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connect-healer
spec:
  template:
    spec:
      containers:
        - name: healer
          env:
            - name: SSL_CERT_FILE
              value: /etc/healer/trust/ca-bundle.pem
          volumeMounts:
            - name: corporate-ca-trust
              mountPath: /etc/healer/trust
              readOnly: true
      volumes:
        - name: corporate-ca-trust
          configMap:
            name: kafka-connect-healer-trust
            defaultMode: 0444
```

Review and apply the rendered configuration:

```sh
kubectl kustomize config/kubernetes-corporate-ca
kubectl apply -k config/kubernetes-corporate-ca
kubectl rollout status deployment/kafka-connect-healer
```

Kustomize gives the generated ConfigMap a content-based name and updates the
Deployment's volume reference. Updating the copied bundle and reapplying the
overlay therefore replaces the pod. See [Kustomize's ConfigMap generation
documentation](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/#generating-resources).

## CA rotation

Restart the process after changing its trust bundle; replacing a mounted file or
updating cluster settings through the API does not reload system trust. For Docker,
recreate the container. For the Kustomize overlay, copy the updated combined bundle
into the overlay directory and apply it again. If another controller manages the
ConfigMap without changing the pod template, arrange a pod restart separately.
