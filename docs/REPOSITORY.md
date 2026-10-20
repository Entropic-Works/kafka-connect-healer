# Repository administration

## Branches and contributions

`main` is the default and release branch: visitors and fresh clones see promoted
release code. `dev` is the target for contributor and dependency-update pull
requests. `staging` holds the next release candidate. There is no separate
`production` branch.

Promote through pull requests in this order:

```text
feature or fix → dev → staging → main → version tag
```

Use **Create a merge commit** for all PRs. Squash and rebase merging are disabled.
Squashing or rebasing long-lived
branches breaks shared ancestry and makes later promotions harder to review.
Automatic branch deletion stays disabled so promotion branches are retained.
These branches represent source promotion; no application deployment is automated.

| Event | Formatting, build, vet, race tests, vulnerabilities | Container smoke test and Kubernetes render | Real Kafka/Connect integration |
| --- | --- | --- | --- |
| Push to a feature/fix branch | Formatting, build, unit tests only | No | No |
| Push to `dev`, `staging`, or `main` | Yes | Yes | No |
| PR into `dev` | Yes | Yes | Yes |
| PR `dev` → `staging` | Yes | Yes | Yes |
| PR `staging` → `main` | Yes | Yes | Yes |
| Version tag release | Yes | Yes | Yes |

The `branch-policy` check rejects PRs into `staging` from anything except this
repository's `dev`, and into `main` from anything except its `staging`.
Forks contribute to `dev`. CI uses `pull_request` with read-only permissions;
contributor code is not run with publishing credentials.
Changing a PR's target branch reruns full validation. Ordinary title/body edits
do not rerun tests; their required gate verifies prior full CI on the same PR,
source commit, and base commit. A newer failed full run cannot reuse older success.
Changing the recorded promotion
source commit does rerun full validation. Metadata edits cannot cancel an active
code-validation run.

Require the **CI required** check from GitHub Actions on all three promotion
branches. It succeeds only when every applicable job passes, including integration
tests on every PR into `dev`, `staging`, or `main`; a failed or canceled job
cannot turn into a passing gate.
Push and release runs use the distinct gate name `CI branch or release`, so a
successful push check cannot stand in for PR integration validation.
The aggregate gate requires integration on all PRs; individual jobs need not be
listed separately in the ruleset.

## Protections and access

The promotion branches require a PR, resolved review conversations, and passing
CI. Direct pushes, force pushes, and deletion are blocked on all three branches.
With one person maintaining two accounts, reviews are requested through CODEOWNERS
but no approval count is required. When an independent maintainer joins, require
one approval, dismiss stale approvals, and require code owner review.

Require feature/fix PRs into `dev` to be up to date with `dev` before merging.
Update the feature branch from `dev` and wait for fresh full CI after another
PR merges. The separate `dev-current-base` ruleset adds this requirement without
changing the shared promotion rules.
Keep the up-to-date requirement disabled for `staging` and `main`: their merge
commits would otherwise force reverse merges into the source branch. CI tests
the proposed merge result. Keep merge commits available and do not enforce
linear history on promotion branches.

The promotion rules have no bypass actors, including admins. Repository admins
can still administer the rules, but normal branch updates must use passing PRs.
Only owners/admins may create `v*` release tags;
published tags must not be rewritten or deleted, including by owners.
The separate `tag-ruleset` blocks all tag updates and deletions with no bypass
actors. Tag creation permissions remain in `release-tag-creation`. Administrators
can still edit rulesets; these rules prevent bypass during ordinary Git operations.

## Promotion and candidate snapshots

Run **Actions → Promote → Run workflow** from `main`. Choose `staging` or `main`,
enter the planned stable version (for example `v0.2.0`), and enter the full reviewed
source commit: `dev` for staging, or `staging` for main. The workflow opens or
reuses a PR with the included commits, source SHA, and release validation checklist.
It never merges the PR, creates a tag, or advances the next stage automatically.
Review the PR and merge it only after full CI passes. GitHub may require a
maintainer to approve Actions runs on PRs created by `GITHUB_TOKEN`; approve that
run in Actions so the full required CI executes.

The helper is also available locally with the maintainer's normal GitHub CLI login:

```sh
GH_REPO=Entropic-Works/kafka-connect-healer \
TARGET_BRANCH=staging RELEASE_VERSION=v0.2.0 EXPECTED_SHA=FULL_REVIEWED_DEV_SHA \
bash scripts/promote.sh
```

Promotion PRs record `<!-- promotion-source: FULL_SHA -->` in the body. CI rejects
a promotion if its source branch advances past that snapshot. Reusing an existing
PR preserves its validation notes and requires the same version and source SHA.
Close an obsolete promotion PR and prepare a replacement after reviewing the new
source commit. For a manually created promotion PR, include that marker with the
full reviewed source SHA and record the same release validation information.

An open `staging` → `main` PR freezes the candidate. Both the promotion helper and
PR policy check reject a new `dev` → `staging` promotion while it is open. Review
and merge only one promotion at a time; the freeze check observes GitHub state
when CI runs, so do not merge a previously validated staging PR after opening a
candidate PR. The source snapshot also prevents the candidate PR from passing CI
if staging advances.

Candidate fixes still travel through `dev` → `staging`. Close the old candidate PR
before replacing it. Use a short freeze on new feature merges into `dev` while
validating a release that may need fixes. If unrelated work has already entered
`dev`, the strict promotion path includes that work too: explicitly accept and
revalidate the larger candidate, or revert/deactivate that work through a PR into
`dev` before promoting. Do not cherry-pick directly into staging or main.
See [RELEASING.md](RELEASING.md) for the release and hotfix procedure.

Roles and maintainer accounts are listed in [MAINTAINERS.md](../MAINTAINERS.md).
Prefer the smallest role needed; ordinary contributors use forks and need no write
access. Changes to roles and credentials are administrative operations, not PR content.

## Security and dependency maintenance

Enable private vulnerability reporting, the dependency graph, Dependabot security
updates, secret scanning, and secret push protection in GitHub settings. Monitor
alerts and private reports. Scanning is an additional check, not evidence that the
entire history is free of secrets. The Docker Hub secrets are `DOCKERHUB_USERNAME`
and `DOCKERHUB_TOKEN`; never put their values in this repository.

Dependabot checks both Go modules, both Dockerfiles, and GitHub Actions weekly,
targeting `dev`. GitHub security-update PRs always target the default branch,
`main`, regardless of `target-branch` in the version-update configuration. Retarget
those PRs to `dev` before merging, then promote the fix through `staging` and
`main`. A dependency PR directly into `main` fails the promotion-source check.
Review updates normally; there is no automatic dependency merge. Keep the Go
version in both modules, Dockerfile, and README consistent.
Go dependency updates must include tidy `go.mod` and `go.sum` files in the affected
module. CI checks module integrity and tidiness. The Docker build discovers and
packages notices from imported dependencies automatically; review those notices
when adding or replacing dependencies.

## First-time setup or recovery

1. Create `dev`, `staging`, and `main` from the same reviewed source commit.
2. Set the default branch to `main` after the setup has been promoted there.
3. Merge the repository setup PR into `dev`, then promote it into `staging` and
   `main` through PRs using merge commits. Templates and Dependabot settings
   become active after they reach the default branch; release safeguards apply
   only once this workflow reaches the commit being tagged.
4. Protect `dev`, `staging`, and `main` with the shared promotion rules and the exact
   check name `CI required` (GitHub Actions app ID `15368`). Run a PR first if
   GitHub's UI has not yet discovered that check name.
5. Apply `dev-current-base` to `dev` only. Apply `tag-ruleset` with no bypass actors
   and keep release-tag creation restricted to owners/admins. Enable **Allow GitHub
   Actions to create and approve pull requests** in Actions workflow permissions
   for Promote; its token has PR-write access but cannot bypass branch rules.
   Verify security settings and publishing secrets.
6. Follow [RELEASING.md](RELEASING.md) for the first release. Branch setup does not
   publish an image, create a version tag, or deploy the service.

The desired settings and new rulesets are recorded in
[repository-settings.json](../.github/repository-settings.json),
[actions-workflow-permissions.json](../.github/actions-workflow-permissions.json), and
[rulesets/](../.github/rulesets/). GitHub does not apply these files automatically:
apply them through the repository settings UI or REST API when recreating the setup.
Existing rulesets must be inspected and preserved, including general branch and tag
protections. GitHub settings are stored by GitHub, not enforced by these documents. Audit them
after ownership, billing/plan, or default-branch changes. GitHub's references explain
[rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets),
[roles](https://docs.github.com/en/organizations/managing-access-to-your-organizations-repositories/repository-roles-for-an-organization),
and [merge methods](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/about-merge-methods-on-github).
