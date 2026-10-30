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

Run these commands from the repository root with Bash, Git, GitHub CLI (`gh`),
and `jq` installed. Use a clean checkout and a maintainer account with permission
to create and merge PRs and create release tags. The examples use `0.0.5`; choose
an unused version for subsequent releases.

### 1. Authenticate and merge the feature into dev

```sh
gh auth login --hostname github.com
gh auth status
export GH_REPO=Entropic-Works/kafka-connect-healer
export RELEASE_VERSION=0.0.5
git fetch origin --prune --tags
git status --short
```

If already authenticated, skip `gh auth login`. Use `gh auth switch` when the
active account is not the maintainer account. Resolve any local changes before
switching branches. If `GH_TOKEN` or `GITHUB_TOKEN` is set in your shell, check
that it represents the intended account; it can override the stored login.

Open a feature PR targeting `dev`, using a description file for the behavior,
configuration migration, limitations, and validation:

```sh
gh pr create --base dev --head YOUR_FEATURE_BRANCH \
  --title 'Describe the user-facing change' --body-file /path/to/pr-description.md
gh pr checks FEATURE_PR_NUMBER --watch --fail-fast
gh pr view FEATURE_PR_NUMBER --json headRefOid,mergeStateStatus
gh pr merge FEATURE_PR_NUMBER --merge --match-head-commit FULL_REVIEWED_FEATURE_SHA
```

Review the diff and resolve review conversations before merging. `dev` requires
the feature branch to include the current `dev` base. If it is behind, merge
`origin/dev` into the feature branch, resolve conflicts, push, and wait for fresh
PR validation. A successful feature-push run does not satisfy `CI required`.

### 2. Promote dev to staging

Keep new feature merges into `dev` paused while preparing the candidate. Check
that no `staging` → `main` PR is already open, then fetch and review the complete
candidate, including earlier unreleased changes in `dev`:

```sh
gh pr list --state open --base main --head staging
git fetch origin --prune --tags
git switch --detach origin/main
git log --oneline origin/staging..origin/dev
git diff origin/staging...origin/dev
dev_sha=$(git rev-parse origin/dev)
GH_REPO="$GH_REPO" TARGET_BRANCH=staging RELEASE_VERSION="$RELEASE_VERSION" \
  EXPECTED_SHA="$dev_sha" bash scripts/promote.sh
```

Use the PR number printed by the helper. It records the full reviewed source SHA
in `<!-- promotion-source: FULL_SHA -->`; preserve that line in its description.
Record configuration/API migration, known limitations, the tested Kafka/Connect
versions, and any deployment-specific candidate validation. Wait for the full PR
run before editing validation results in the description:

```sh
gh pr checks STAGING_PR_NUMBER --watch --fail-fast
gh pr edit STAGING_PR_NUMBER --body-file /path/to/staging-pr-description.md
gh pr checks STAGING_PR_NUMBER --watch --fail-fast
gh pr merge STAGING_PR_NUMBER --merge --match-head-commit "$dev_sha"
```

Confirm `branch-policy`, `verify`, `container`, `integration`, and `CI required`
pass. The integration job starts real Kafka and Connect containers. Review the
candidate and record any staging-environment validation you performed; merging
the branch itself does not deploy an environment. Never use `--delete-branch`
on promotion PRs: `dev` and `staging` are permanent branches.

### 3. Promote staging to main

After the staging PR is merged, fetch and review the resulting **staging merge
commit**. Its SHA differs from the `dev` SHA used in step 2:

```sh
git fetch origin --prune --tags
git log --oneline origin/main..origin/staging
git diff origin/main...origin/staging
staging_sha=$(git rev-parse origin/staging)
GH_REPO="$GH_REPO" TARGET_BRANCH=main RELEASE_VERSION="$RELEASE_VERSION" \
  EXPECTED_SHA="$staging_sha" bash scripts/promote.sh
gh pr checks MAIN_PR_NUMBER --watch --fail-fast
gh pr edit MAIN_PR_NUMBER --body-file /path/to/main-pr-description.md
gh pr checks MAIN_PR_NUMBER --watch --fail-fast
gh pr merge MAIN_PR_NUMBER --merge --match-head-commit "$staging_sha"
```

Preserve the generated source marker, record validation, and resolve review
conversations as in step 2. This PR runs fresh full CI, including integration.
Opening it freezes staging: do not merge another `dev` → `staging` PR until this
candidate is merged or closed. After merging, follow [Tag and publish](#tag-and-publish).

### Optional: use the Actions UI

**Actions → Promote → Run workflow** accepts branch **main**, target **staging**
or **main**, the planned stable version, and the full reviewed source SHA. Use
the `dev` SHA for target `staging` and the resulting `staging` SHA for target
`main`. Then review, validate, and merge the generated PR as above.

This workflow uses `GITHUB_TOKEN`. It requires the repository setting
**Settings → Actions → General → Workflow permissions → Allow GitHub Actions to
create and approve pull requests**, which is disabled in this repository at the
time of writing. Prefer the local helper using your maintainer login; enabling
that combined setting also grants PR approval capability and requires a separate
repository-policy decision. See [GitHub's workflow-permissions documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository#preventing-github-actions-from-creating-or-approving-pull-requests).
If a bot-created PR's CI requests approval, approve its run in Actions and wait
for all checks. See [GitHub's token event behavior](https://docs.github.com/en/actions/concepts/security/github_token).

### If promotion or CI fails

Inspect the failing job rather than rerunning the aggregate `CI required` job:

```sh
gh run list --workflow ci.yml --event pull_request --limit 10
gh run view RUN_ID --log-failed
```

| Symptom | Action |
| --- | --- |
| Promote cannot create a PR with `GITHUB_TOKEN` | Use the local helper with your maintainer login. Check the Actions setting described above; do not weaken branch protections. |
| `dev moved` or `staging moved` | Fetch and review the new source commit, then rerun the helper with its full SHA. |
| Candidate changed or has no source snapshot | Close the stale candidate PR and prepare a replacement using the helper. Do not change its marker just to bypass the check. |
| Staging is frozen | Complete or close the existing `staging` → `main` PR before preparing another staging candidate. |
| `CI required` is expected | A branch-push run is insufficient. Check for a pending approval, merge conflict, or missing full PR run. If no run exists, close and reopen the unmerged PR using your maintainer login to trigger full validation. |
| Metadata job reports no successful full CI for the current commits | Let an in-flight full PR run finish. If none succeeded for the current source and base, close and reopen the unmerged PR to trigger one, then update its description after it passes. Rerunning only the metadata job cannot replace full validation. |
| Build, vulnerability, container, or integration check fails | Fix the cause through the feature → `dev` path and validate again. Integration diagnostics are attached to the failed run. For a confirmed transient failure, use `gh run rerun RUN_ID --failed`. |
| PR into `dev` is behind | Merge current `origin/dev` into the feature branch and push; wait for new PR checks. |

Do not bypass a failed required check or use a direct feature PR into staging or
main. Candidate fixes follow the process below.

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
pull image digests anonymously and confirm the package is publicly accessible.

Use a clean checkout and fetch current branches and tags:

```sh
git fetch origin --tags
git switch --detach origin/main
git tag -a "$RELEASE_VERSION" -m "Kafka Connect Healer $RELEASE_VERSION"
git push origin "$RELEASE_VERSION"
gh run list --workflow release.yml --branch "$RELEASE_VERSION" --limit 1
gh run watch RELEASE_RUN_ID --exit-status
gh release view "$RELEASE_VERSION"
```

Set `GH_REPO` and `RELEASE_VERSION` as in step 1 if using a new shell. Confirm the
version does not already exist with `git tag --list "$RELEASE_VERSION"` and
`gh release list` before creating it. Review the fetched main commit and wait for
its branch CI as well. The **Release** run must finish successfully, including
both native image-verification jobs, before announcing the release. If tag
creation is denied, use a maintainer account allowed by the release-tag rules;
do not change or bypass the rules to publish.

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

## After release

Supplement the generated release notes with user-facing changes and migration
instructions, preserving the verified digests and validation link:

```sh
gh release view "$RELEASE_VERSION" --json body --jq .body > /path/to/release-notes.md
# Edit that file, then publish the complete notes.
gh release edit "$RELEASE_VERSION" --notes-file /path/to/release-notes.md
```

Close the completed feature issue with a link to the merged PR or release and
record any proposals that were explicitly deferred. Delete obsolete feature
branches only after checking their commits are included in `origin/main` and
they have no open PR. Keep `dev`, `staging`, `main`, and release tags:

```sh
git fetch origin --prune --tags
git branch -r --merged origin/main
gh pr list --state open --head COMPLETED_FEATURE_BRANCH
git merge-base --is-ancestor origin/COMPLETED_FEATURE_BRANCH origin/main
git push origin --delete COMPLETED_FEATURE_BRANCH
git switch --detach origin/main
git branch -d COMPLETED_FEATURE_BRANCH
git fetch origin --prune
```
