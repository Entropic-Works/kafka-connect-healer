# Releasing Kafka Connect Healer

Release versions use `MAJOR.MINOR.PATCH`; prereleases add a suffix such as
`0.1.0-rc.1`. Leading zeros and empty prerelease identifiers are rejected. Build
metadata (`+...`) is not supported for Docker tags. Never reuse or move a published tag.

Stable release commits must be reachable from `main`. Prerelease commits
must be reachable from `staging`. The release workflow checks this before running
the full CI suite, then publishes Linux amd64 and arm64 images to GHCR as
`ghcr.io/entropic-works/kafka-connect-healer:<version>`, with an SBOM and build provenance.
There are no floating `latest`, major, or minor tags. Stable and prerelease images
use the same publishing credentials.
The workflow then pulls each published platform digest and runs the container smoke
suite on native amd64 and arm64 runners. It creates a GitHub release only after
both platforms pass, including the image index and platform digests in the notes.
Prerelease tags produce GitHub prereleases.

## Prepare and promote

1. Review the application behavior, dependency updates, and known
   limitations. Verify `LICENSE`, `NOTICE`, and dependency notices are included.
2. Merge contributions into `dev` through passing PRs, including the real
   Kafka/Connect integration suite.
3. Run **Actions → Promote** from `main`, choosing `staging`, the planned stable
   release version, and the full reviewed `dev` commit SHA. Review the generated PR,
   record release-specific validation, and merge using **Create a merge commit**.
   Wait for full CI, including integration tests, and review the candidate and any
   validation performed in your staging environment.
   The branch itself does not deploy an environment.
4. Record the resulting staging commit. For a stable release, run **Promote** with
   target `main`, the same version, and that full staging SHA. Opening this PR
   freezes staging. Review and merge `staging` → `main` in the same way.
   This promotion reruns the full CI suite, including integration tests.
   Fixes to candidates follow the same `dev` → `staging` path. Production hotfixes
   also follow this path; do not bypass integration tests with a direct PR into main.

The workflow refuses stale source SHAs and preserves an existing matching PR's
notes. Approve any Actions run approval requested by GitHub for a bot-created PR.
The [repository workflow](REPOSITORY.md#promotion-and-candidate-snapshots) also
documents local use of `scripts/promote.sh` with a maintainer's GitHub CLI login.

## Candidate fixes and hotfixes

Use a short freeze on new feature merges into `dev` while a release candidate is
being validated. A candidate is identified by its full staging commit and,
optionally, an immutable prerelease tag such as `0.2.0-rc.1`. Keep staging fixed
while its PR into main is open. Do not reuse a prerelease tag for a replacement.

If the candidate needs a fix:

1. Close its PR into main, preserving the validation notes and reason for replacement.
2. Branch from `dev`, merge the fix through a passing PR into `dev`, and rerun
   Promote into staging with the newly reviewed `dev` SHA.
3. Validate the replacement staging commit, use a new candidate tag if needed, and
   rerun Promote into main with that staging SHA.

If a promotion PR's source moves before it merges, close it and prepare a new PR
with the new SHA. CI rejects the old snapshot.

Hotfixes use the same steps and full integration gates. If dev already contains
unreleased features, explicitly include and validate them, or revert/deactivate
them through reviewed PRs into dev before promoting the hotfix. This workflow does
not provide isolated patch releases while keeping unrelated changes in dev.

## Tag and publish

Release jobs publish to `ghcr.io/entropic-works/kafka-connect-healer` using
GitHub Actions' `GITHUB_TOKEN` with `packages: write`. The package is public and
linked to this repository through the image's source label. Verification jobs
have `packages: read` and authenticate to GHCR before pulling image digests.

Use a clean checkout and fetch current branches and tags:

```sh
git fetch origin --tags
git switch --detach origin/main
git tag -a 0.1.0 -m 'Kafka Connect Healer 0.1.0'
git push origin 0.1.0
```

For a candidate, detach at `origin/staging` instead and use a prerelease tag.
Tag only a reviewed commit containing the release workflow and all release inputs.
Monitor the **Release** workflow. Image publication happens only after full CI
passes on that exact tag. The images are available in GHCR before the native
smoke checks finish; GitHub release announcement waits for both checks. If checks
fail before publication, fix and promote a new commit and use a new version tag.
Do not delete/repoint tags to retry different code. For a transient verification
failure after publication, rerun only the failed verification/announcement jobs;
do not rebuild an already published version to change its contents.

The workflow generates a GitHub release with linked changes, source SHA, validation
run, and verified digests. Review and supplement those generated notes with:

- User-facing changes and links to relevant PRs.
- Configuration and API behavior, including compatibility requirements.
- The Kafka Connect versions and scenarios actually tested.
- Known limitations and the Docker image reference and digest.

Standalone binary archives are not generated. Users can build from the tagged source
using the source-build instructions in the README. Fresh builds download Go
dependencies; subsequent builds can reuse the module cache.

The release workflow verifies published images with the documented fixtures,
including API access, health probes, authentication, and graceful shutdown.
Verify any additional deployment-specific configuration in your environment.
For reproducible deployments, use the recorded digest.
