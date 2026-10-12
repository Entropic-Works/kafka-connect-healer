# Releasing Kafka Connect Healer

Release versions use `vMAJOR.MINOR.PATCH`; prereleases add a suffix such as
`v0.1.0-rc.1`. Leading zeros and empty prerelease identifiers are rejected. Build
metadata (`+...`) is not supported for Docker tags. Never reuse or move a published tag.

Stable release commits must be reachable from `main`. Prerelease commits
must be reachable from `staging`. The release workflow checks this before running
the full CI suite, then publishes Linux amd64 and arm64 images to Docker Hub as
`entropicworks/kafka-connect-healer:<version>`, with an SBOM and build provenance.
There are no floating `latest`, major, or minor tags. Stable and prerelease images
use the same publishing credentials.

## Prepare and promote

1. Review the changes, migration requirements, dependency updates, and known
   limitations. Verify `LICENSE`, `NOTICE`, and dependency notices are included.
2. Merge contributions into `dev` through passing PRs, including the real
   Kafka/Connect integration suite.
3. Open and merge a PR from `dev` into `staging` using **Create a merge commit**.
   Wait for full CI, including integration tests, and review the candidate and any
   validation performed in your staging environment.
   The branch itself does not deploy an environment.
4. For a stable release, open and merge `staging` → `main` in the same way.
   This promotion reruns the full CI suite, including integration tests.
   Fixes to candidates follow the same `dev` → `staging` path. Production hotfixes
   also follow this path; do not bypass integration tests with a direct PR into main.

## Tag and publish

Ensure `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` are configured, the destination
Docker Hub repository is public, and private vulnerability reporting is enabled.
Use a clean checkout and fetch current branches and tags:

```sh
git fetch origin --tags
git switch --detach origin/main
git tag -a v0.1.0 -m 'Kafka Connect Healer v0.1.0'
git push origin v0.1.0
```

For a candidate, detach at `origin/staging` instead and use a prerelease tag.
Tag only a reviewed commit containing the release workflow and all release inputs.
Monitor the **Release** workflow; publication happens only after full CI passes on
that exact tag. If checks fail before publication, fix and promote a new commit
and use a new version tag. Do not delete/repoint tags to retry different code.

After publication, create a GitHub release for the tag. Mark candidate releases
as prereleases. Include:

- User-facing changes and links to relevant PRs.
- Configuration/API compatibility changes and required migration steps.
- The Kafka Connect versions and scenarios actually tested.
- Known limitations and the Docker image reference and digest.

The workflow publishes images only. Standalone binary archives and GitHub release
pages are not generated automatically. Users can build from the tagged source
using the offline build instructions in the README.

Verify the published image can be pulled and starts with the documented examples;
record its digest in the release notes. For reproducible deployments, use the digest
rather than relying on a mutable registry tag.
