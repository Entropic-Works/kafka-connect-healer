# Repository administration

## Branches and contributions

`dev` is the default branch and the target for contributor and dependency-update
pull requests. `staging` holds the next release candidate. `production` holds
promoted release code. The former default, `main`, is retained for history; do not
use it for new development or releases.

Promote through pull requests in this order:

```text
feature or fix → dev → staging → production → version tag
```

Use **Create a merge commit** for all PRs. Squash and rebase merging are disabled.
Squashing or rebasing long-lived
branches breaks shared ancestry and makes later promotions harder to review.
Automatic branch deletion stays disabled so promotion branches are retained.
These branches represent source promotion; no application deployment is automated.

| Event | Formatting, build, vet, race tests, vulnerabilities | Container smoke test and Kubernetes render | Real Kafka/Connect integration |
| --- | --- | --- | --- |
| Push to a branch | Yes | Yes | No |
| PR into `dev` | Yes | Yes | Yes |
| PR `dev` → `staging` | Yes | Yes | No |
| PR `staging` → `production` | Yes | Yes | No |
| Version tag release | Yes | Yes | Yes |

The `branch-policy` check rejects PRs into `staging` from anything except this
repository's `dev`, and into `production` from anything except its `staging`.
Forks contribute to `dev`. CI uses `pull_request` with read-only permissions;
contributor code is not run with publishing credentials.

Require the **CI required** check from GitHub Actions on all three promotion
branches. It succeeds only when every applicable job passes, including integration
tests on PRs into `dev`; a failed or canceled job cannot turn into a passing gate.
Do not require the conditional `integration` job separately on staging/production.

## Protections and access

The promotion branches require a PR, resolved review conversations, and passing
CI. Force pushes and deletion are blocked. Keep the legacy `main` protected too.
With one person maintaining two accounts, reviews are requested through CODEOWNERS
but no approval count is required. When an independent maintainer joins, require
one approval, dismiss stale approvals, and require code owner review.

Do not require branches to be up to date before merging: that can force reverse
merges during promotions. CI tests the proposed merge result. Keep merge commits
available and do not enforce linear history on promotion branches.

Existing owner/admin bypass access is retained for emergency recovery. Routine
changes still go through PRs. Only owners/admins may create `v*` release tags;
published tags must not be rewritten or deleted, including by owners.

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
targeting `dev`. GitHub security-update PRs target the default branch, so make sure
that it remains `dev`. Review updates normally; there is no automatic dependency
merge. Keep the Go version in both modules, Dockerfile, and README consistent.
Root Go dependency updates must also refresh `vendor/` and preserve notices.

## First-time setup or recovery

1. Create `dev`, `staging`, and `production` from the same reviewed source commit.
2. Set the default branch to `dev`. Retain `main`.
3. Merge the repository setup PR into `dev`, then promote it into `staging` and
   `production` through PRs using merge commits. Templates and Dependabot settings
   become active after they reach the default branch; release safeguards apply
   only once this workflow reaches the commit being tagged.
4. Protect `dev`, `staging`, and `production` with the rules above and the exact
   check name `CI required` (GitHub Actions app ID `15368`). Run a PR first if
   GitHub's UI has not yet discovered that check name.
5. Keep release-tag creation restricted to owners/admins and retain existing tag
   deletion/force-update protection. Verify security settings and publishing secrets.
6. Follow [RELEASING.md](RELEASING.md) for the first release. Branch setup does not
   publish an image, create a version tag, or deploy the service.

The desired settings and new rulesets are recorded in
[repository-settings.json](../.github/repository-settings.json) and
[rulesets/](../.github/rulesets/). GitHub does not apply these files automatically:
apply them through the repository settings UI or REST API when recreating the setup.
Existing rulesets must be inspected and preserved, including legacy branch and tag
protections. GitHub settings are stored by GitHub, not enforced by these documents. Audit them
after ownership, billing/plan, or default-branch changes. GitHub's references explain
[rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/about-rulesets),
[roles](https://docs.github.com/en/organizations/managing-access-to-your-organizations-repositories/repository-roles-for-an-organization),
and [merge methods](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/about-merge-methods-on-github).
