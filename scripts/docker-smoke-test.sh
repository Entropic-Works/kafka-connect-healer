#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
image="${1:-kafka-connect-healer:local}"
container_id=""
api_url=""
health_url=""

cleanup() {
  result=$?
  if [[ -n "$container_id" ]]; then
    if [[ "$result" -ne 0 ]]; then
      docker logs "$container_id" >&2 || true
    fi
    docker rm --force "$container_id" >/dev/null || true
  fi
}
trap cleanup EXIT

start_container() {
  container_id=$(docker run --detach --read-only --cap-drop=ALL \
    --security-opt=no-new-privileges --publish 127.0.0.1::8080 --publish 127.0.0.1::8081 \
    --mount "type=bind,src=$PWD/examples/application.yaml,dst=/config/application.yaml,readonly" \
    --mount "type=bind,src=$PWD/examples/empty-clusters.yaml,dst=/config/connect-clusters.yaml,readonly" \
    --mount "type=bind,src=$PWD/tests/fixtures/docker/application-secret.yaml,dst=/config/application-secret.yaml,readonly" \
    "$image" --config=/config/application.yaml \
    --connect-clusters=/config/connect-clusters.yaml "$@")
  api_url="http://$(docker port "$container_id" 8080/tcp)"
  health_url="http://$(docker port "$container_id" 8081/tcp)"
  for attempt in {1..60}; do
    if curl --fail --silent --output /dev/null --max-time 1 "$health_url/readyz"; then
      return
    fi
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container_id")" != true ]]; then
      break
    fi
    sleep 0.5
  done
  echo "Container did not become ready" >&2
  return 1
}

stop_container() {
  docker stop --time 10 "$container_id" >/dev/null
  [[ "$(docker inspect --format '{{.State.ExitCode}}' "$container_id")" == 0 ]]
  docker logs "$container_id" 2>&1 | grep -q 'Kafka Connect Healer stopped'
  docker rm "$container_id" >/dev/null
  container_id=""
}

start_container
[[ "$(docker inspect --format '{{.Config.User}}' "$container_id")" == '65532:65532' ]]
docker cp "$container_id:/licenses/kafka-connect-healer-NOTICE" - | tar -xOf - | diff NOTICE -
curl --fail --silent --show-error --max-time 5 "$api_url/" | jq -e 'any(.[]; .method == "PUT" and .path == "/clusters/{name}")' >/dev/null
curl --fail --silent --show-error --max-time 5 "$api_url/config" | jq -e '.pollingBehavior.interval == "10s" and (.apiConfig.authConfig | has("password") | not)' >/dev/null
curl --fail --silent --show-error --max-time 5 "$api_url/clusters" | jq -e '. == {}' >/dev/null
[[ "$(curl --silent --show-error --max-time 5 --output /dev/null --write-out '%{http_code}' \
  --request PATCH --header 'Content-Type: application/json' \
  --data '{"pollingBehavior":{"interval":"1h"}}' "$api_url/config")" == 204 ]]
curl --fail --silent --show-error --max-time 5 "$api_url/config" | jq -e '.pollingBehavior.interval == "1h0m0s"' >/dev/null
[[ "$(curl --silent --show-error --max-time 5 --output /dev/null --write-out '%{http_code}' \
  --request PUT --header 'Content-Type: application/json' \
  --data '{"host":"localhost","port":"8083"}' "$api_url/clusters/smoke-test")" == 201 ]]
curl --fail --silent --show-error --max-time 5 "$api_url/clusters" | jq -e '."smoke-test".host == "localhost"' >/dev/null
[[ "$(curl --silent --show-error --max-time 5 --output /dev/null --write-out '%{http_code}' \
  --request DELETE "$api_url/clusters/smoke-test")" == 204 ]]
curl --fail --silent --show-error --max-time 5 "$api_url/clusters" | jq -e '. == {}' >/dev/null
stop_container

start_container --secret-config=/config/application-secret.yaml
for route in / /config /clusters; do
  [[ "$(curl --silent --show-error --max-time 5 --output /dev/null --write-out '%{http_code}' "$api_url$route")" == 401 ]]
done
curl --fail --silent --show-error --max-time 5 --user smoke-test:smoke-test-only "$api_url/config" \
  | jq -e '.apiConfig.authConfig.enabled == true and (.apiConfig.authConfig | has("password") | not)' >/dev/null
for route in /livez /readyz; do
  [[ "$(curl --fail --silent --show-error --max-time 5 "$health_url$route")" == ok ]]
  [[ "$(curl --silent --show-error --max-time 5 --user smoke-test:smoke-test-only \
    --output /dev/null --write-out '%{http_code}' "$api_url$route")" == 404 ]]
done
for route in / /config /clusters; do
  [[ "$(curl --silent --show-error --max-time 5 --output /dev/null \
    --write-out '%{http_code}' "$health_url$route")" == 404 ]]
done
stop_container
echo 'Docker smoke tests passed'
