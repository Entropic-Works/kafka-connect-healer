# Contributing

Kafka Connect Healer is maintained by Entropic Works, Inc. Bug reports and focused
pull requests are welcome at https://github.com/Entropic-Works/kafka-connect-healer.
Discuss larger behavior or API changes in an issue before implementing them.

Fork the repository, branch from `dev`, and open your pull request against `dev`.
Use a focused branch name such as `fix/restart-timeout` or `docs/configuration`.
Maintainers promote `dev` to `staging`, then `staging` to `production` using merge
commits; contributors do not target those branches. See the
[repository workflow](docs/REPOSITORY.md) and [code of conduct](CODE_OF_CONDUCT.md).

For bugs, include the version/image tag, relevant configuration with credentials
removed, Kafka Connect version, reproduction steps, and sanitized logs. Do not post
security vulnerabilities publicly; follow [SECURITY.md](SECURITY.md).

## Development

Use the Go version in `go.mod`. Application code lives under `internal/`, and the
entrypoint is `cmd/kafka-connect-healer`. Follow the existing style, use `gofmt`,
keep changes focused, and add tests for changed behavior. Update the README and
examples when configuration or API behavior changes.

```sh
gofmt -w cmd internal
go mod verify
bash scripts/test-repository-policy.sh
go build ./...
go vet ./...
go test -race -shuffle=on -count=3 -timeout=5m ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
docker build -t kafka-connect-healer:local .
bash scripts/docker-smoke-test.sh kafka-connect-healer:local
go -C tests/integration test -race -count=1 -timeout=20m ./...
```

The container smoke test requires Docker, Bash, curl, jq, tar, and diff. Go tests
start local HTTP servers. Vendored dependencies are checked in; after intentionally
changing dependencies, run `go mod tidy` and `go mod vendor`. Do not edit `vendor/`
by hand. The vulnerability check downloads its tool and accesses the Go vulnerability
database, even though application dependencies are vendored.

The [integration suite](tests/integration/README.md) requires a working Docker
daemon and downloads its own test-only dependencies. It runs in CI on pull
requests into `dev` and for release tags. Branch pushes and promotion PRs run the
fast checks and container smoke test. Its separate module is not vendored into
the production application. Run `go mod tidy` inside `tests/integration/` when
changing those dependencies; do not run `go mod vendor` there. Its Go test setup
builds current application/fixture images automatically. Docker's CLI and
BuildKit are required; Bash is not required for this suite. Use an isolated local
Docker daemon, not a production Docker host.

Contributions are submitted under the project's Apache-2.0 license. Contributors
retain ownership of their contributions; submitting a pull request does not assign
copyright to Entropic Works. Only submit code you have the right to contribute, and
preserve third-party license notices.

Dependency-update pull requests follow the same review process. For updates to
the root Go module, verify that `vendor/` was regenerated and its notices are
preserved. Never commit actual runtime configuration or credentials; local files
under `config/` and files named `*-secret.yaml`/`*-secret.yml` are ignored. Public
test fixtures under `tests/fixtures/` are intentional exceptions.
