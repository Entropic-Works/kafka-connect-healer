# Repository workflow

## Branches and contributions

`main` is the default branch and contains promoted release code. Contributions
and dependency updates target `dev`. `staging` holds the release candidate.

Changes follow this path through pull requests:

```text
feature or fix → dev → staging → main → version tag
```

All pull requests use merge commits. Feature branches must be up to date with
`dev` before merging. Protected branches require passing CI and resolved review
conversations. Release tags are immutable.

## Continuous integration

| Event | Checks | Real Kafka/Connect integration |
| --- | --- | --- |
| Feature branch push | Formatting, module integrity, build, unit tests | No |
| Push to `dev`, `staging`, or `main` | Formatting, module integrity, build, vet, race tests, vulnerability checks, container smoke test, Kubernetes render | No |
| PR into `dev`, `staging`, or `main` | All checks above | Yes |
| Release tag | All checks above and native amd64/arm64 image verification | Yes |

The required `CI required` check passes only when every applicable PR check
succeeds. Each promotion runs fresh integration tests. Contributor CI runs without
publishing credentials. A title or description edit can reuse successful CI only
when the PR, source commit, and base commit remain unchanged.

## Promotion and candidate snapshots

Maintainers run **Actions → Promote** from `main`, choosing the target branch,
planned stable version, and full source commit SHA. The source is `dev` for a
promotion to `staging`, or `staging` for a promotion to `main`.

The helper is also available through GitHub CLI:

```sh
GH_REPO=Entropic-Works/kafka-connect-healer \
TARGET_BRANCH=staging RELEASE_VERSION=0.2.0 EXPECTED_SHA=FULL_REVIEWED_DEV_SHA \
bash scripts/promote.sh
```

Each promotion PR records `<!-- promotion-source: FULL_SHA -->` and its validation
results. CI rejects a candidate whose source branch has advanced. An open
`staging` → `main` PR freezes staging; complete or close that PR before preparing
another candidate. Candidate fixes follow the same `dev` → `staging` path.

Branch promotion does not deploy the application. See [RELEASING.md](RELEASING.md)
for tagging and publication.

## Dependency maintenance

Dependabot checks both Go modules, both Dockerfiles, and GitHub Actions weekly,
targeting `dev`. Retarget security-update PRs to `dev` when GitHub opens them
against the default branch.

Review module-file changes and dependency licenses. Run `go mod tidy` in the
module whose dependencies changed, and commit its `go.mod` and `go.sum`. CI checks
module integrity and tidiness. Docker builds collect notices for imported
production dependencies automatically. Keep the Go version consistent across
both modules, Dockerfile, and README.

See [CONTRIBUTING.md](../CONTRIBUTING.md), [MAINTAINERS.md](../MAINTAINERS.md), and
[SECURITY.md](../SECURITY.md) for contribution, governance, and security policies.
