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
| Feature branch push | Formatting, module integrity, workflow syntax, repository policies, build, vet, unit tests | No |
| Push to `dev`, `staging`, or `main` | Formatting, module integrity, build, vet, race tests, vulnerability checks, container smoke test, Kubernetes render | No |
| PR into `dev`, `staging`, or `main` | All checks above | Yes |
| Release tag | All checks above and native amd64/arm64 image verification | Yes |

The required `CI required` check passes only when every applicable PR check
succeeds. Each promotion runs fresh integration tests. Contributor CI runs without
publishing credentials. A title or description edit can reuse successful CI only
when the PR, source commit, and base commit remain unchanged.

**Plan and promotion policy** selects the checks and validates the promotion path,
source snapshot, and staging freeze before expensive jobs start. **Go checks**,
**Container and Kubernetes**, and **Kafka Connect integration** then run in
parallel when required. Full Go validation runs each test once with race detection
and shuffled ordering. **Reuse previous CI** handles metadata edits; it checks all
API pages and accepts the previous job names during migration. A newer failed
full run, including a failed planning run, invalidates older success.

The final gate keeps the protected name `CI required` for PRs and uses
`CI branch or release` for other events. Its job summary lists every upstream
result, and errors identify failed, cancelled, skipped, or missing required checks.
Policy helpers report stable `CI_*` codes locally and as Actions annotations.
Exit statuses are `1` for policy/check failures, `2` for invalid inputs, `3` for
GitHub API/permission failures, and `4` for a validation wait timeout. Tool failures
such as Go compiler errors retain the tool's original diagnostics and exit status.

## Promotion and candidate snapshots

Maintainers run `scripts/promote.sh` locally using their GitHub CLI login. The
source is `dev` for a promotion to `staging`, or `staging` for a promotion to `main`.

```sh
GH_REPO=Entropic-Works/kafka-connect-healer \
TARGET_BRANCH=staging RELEASE_VERSION=0.2.0 EXPECTED_SHA=FULL_REVIEWED_DEV_SHA \
bash scripts/promote.sh
```

Follow the [step-by-step promotion runbook](RELEASING.md#prepare-and-promote) for
authentication, reviewing the source SHA, monitoring CI, merging, and recovering
from failures. The optional **Actions → Promote** workflow uses `GITHUB_TOKEN`;
it cannot create PRs while the repository's **Allow GitHub Actions to create and
approve pull requests** setting is disabled. Use the local helper with your
maintainer login in that configuration.

Each promotion PR records `<!-- promotion-source: FULL_SHA -->` and its validation
results. LF and CRLF descriptions are equivalent; missing, duplicate, and changed
source markers have distinct errors. CI rejects a candidate whose source branch
has advanced. An open
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
