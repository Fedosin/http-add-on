# Contributing to KEDA HTTP Add-on (OpenShift Downstream)

This document covers contribution guidelines specific to the OpenShift downstream fork of [KEDA HTTP Add-on](https://github.com/kedacore/http-add-on).
For upstream contribution guidelines, see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Related Resources

| Resource | Link |
| -------- | ---- |
| Upstream repo | [kedacore/http-add-on](https://github.com/kedacore/http-add-on) |
| Upstream KEDA repo | [kedacore/keda](https://github.com/kedacore/keda) |
| Downstream KEDA repo | [openshift/kedacore-keda](https://github.com/openshift/kedacore-keda) |
| CMA operator repo | [openshift/custom-metrics-autoscaler-operator](https://github.com/openshift/custom-metrics-autoscaler-operator) |
| CI configuration | [openshift/release/.../kedacore-http-add-on/](https://github.com/openshift/release/tree/master/ci-operator/config/openshift/kedacore-http-add-on) |
| Release pipeline (Konflux) | [custom-metrics-autoscaler-pipelines](https://github.com/openshift/custom-metrics-autoscaler-pipelines) |
| AI guidance | [AGENTS.md](./AGENTS.md) |
| OpenShift docs (CMA install/config) | [Custom Metrics Autoscaler](https://docs.openshift.com/container-platform/latest/nodes/cma/nodes-cma-autoscaling-custom.html) |
| User docs (HTTP Add-on, upstream only) | [keda.sh/http-add-on](https://keda.sh/http-add-on/) |

## Review and Approval Policy

Every change in every pull request must be understood and approved by two humans.
This can be the PR author and a reviewer, or — if the author used an AI tool and does not fully understand the contents of the PR — two human reviewers.

**Exception:** PRs authored by deterministic automation tools that are part of our CI and related systems (whose code has been reviewed by the OpenShift engineering org) can be merged with a single human review.

Every change should be closely scrutinized for bugs.
Our software is complex with many interdependencies.
Review changes from multiple angles:

- **Product architecture**: Does this fit the intended design of KEDA HTTP Add-on and OpenShift?
- **Security**: Are there new attack surfaces, credential handling issues, or privilege escalations?
- **Thread safety**: The interceptor handles concurrent HTTP requests — are shared resources properly synchronized?
- **Regressions**: Could this break existing routing, scaling, or metrics behavior?
- **Effects on other components**: How does this impact the CMA operator, KEDA core, or the other binaries in this repo?

## Upstream Commit Convention

This is a downstream fork.
All non-upstream commits must use one of the following prefixes to ensure changes are not lost during the next upstream rebase:

- `UPSTREAM: <carry>:` — A change that should be kept (carried) indefinitely, or as long as it makes sense to do so
- `UPSTREAM: <drop>:` — A change that should be discarded during the next rebase cycle
- `UPSTREAM: 1234:` — A change carried until the rebase includes upstream PR #1234

Examples:

```
UPSTREAM: <carry>: Switch to OCP base images
UPSTREAM: <drop>: Add local golangci-lint and helm auto-install to Makefile
UPSTREAM: 1751: Add SecurityContext to e2e test pods for PSA restricted
```

These prefixes are for commit titles, not PR titles.

## Upstream-First Policy

New feature work should be directed to the [upstream KEDA HTTP Add-on project](https://github.com/kedacore/http-add-on).
Downstream-only features are discouraged due to the ongoing cost of maintaining them through each rebase cycle.
If a downstream-only change is necessary, use the `UPSTREAM: <carry>:` prefix and include a comment in the PR explaining why it cannot go upstream.

## PR Title Convention

PR titles should be prefixed with a Jira ticket reference:

```
AUTOSCALE-123: Fix interceptor routing for host-based matching
OCPBUGS-456: Correct nil pointer in operator reconcile loop
NO-JIRA: Update Go module dependencies
```

The Jira prefix goes in the **PR title**.
The upstream commit prefix goes in the **commit message**.

## PR Workflow

This repo uses [OpenShift CI (Prow)](https://docs.ci.openshift.org/) for continuous integration.
GitHub Actions workflows in this repo are from upstream and are **not used** for our CI.
PRs are automatically merged once all required tests pass and the correct labels are present.

### Required labels for merge

- `lgtm` — Added by a reviewer via the `/lgtm` command. Any developer from the OpenShift org can add this after reviewing the PR.
- `approved` — Added by an approver listed in the [OWNERS](./OWNERS) file via the `/approve` command.

### Useful commands

Comment these on the PR:

| Command | Effect |
| ------- | ------ |
| `/lgtm` | Add the `lgtm` label after reviewing. Since this repo is onboarded to [Pipeline Controller LGTM mode](https://docs.ci.openshift.org/how-tos/creating-a-pipeline/#the-pipeline-required-command), this also triggers the e2e test |
| `/lgtm cancel` | Remove the `lgtm` label |
| `/approve` | Add the `approved` label (OWNERS approvers only) |
| `/pipeline required` | Manually trigger second-stage tests (the e2e test) without waiting for `/lgtm` |
| `/retest` | Re-run all failed required tests |
| `/retest-required` | Re-run only the failed required tests |
| `/test <test-name>` | Run a specific test, e.g. `/test http-addon-e2e-aws` |
| `/hold` | Prevent the PR from being merged |
| `/hold cancel` | Remove the hold and allow merging |
| `/verified` | Mark the PR as verified |
| `/cherry-pick release-4.18` | Create a cherry-pick PR to a release branch |

### LGTM mode and e2e tests

This repo promotes images into the OpenShift release payload, so it's onboarded to Pipeline Controller LGTM mode.
The e2e test (`http-addon-e2e-aws`) is a second-stage test: it doesn't run automatically on every push, only once the `/lgtm` label is applied.
Lint, unit tests, and `verify-history` are first-stage and still run on every push.
If you need the e2e test to run before getting `/lgtm` (e.g., to validate before requesting review), use `/pipeline required`.

### Preventing premature merges

- Add the `WIP:` prefix to the PR title (e.g., `WIP: AUTOSCALE-123: Work in progress`).
  Prow adds the `do-not-merge/work-in-progress` label automatically.
- Use `/hold` to temporarily block merging while awaiting additional review or testing.

## Test Expectations

PRs should include tests to verify correctness and prevent future regressions:

- **Unit tests**: Required for new logic, bug fixes, and behavior changes. Run with `make test`.
- **E2E tests**: Expected for new features or significant behavior changes. E2E tests are organized by profile (`default/`, `observability/`, `tls/`) in `test/e2e/`. Run locally with `make e2e-test`.

## Verified Label

Use `/verified` to indicate changes have been verified.
Examples:

```
/verified
/verified by unit tests
/verified by E2Es
/verified deferred to QE
```

## Generated Code

The following files are generated and should never be hand-edited:

| File(s) | Generator | Regenerate with |
| ------- | --------- | --------------- |
| `**/zz_generated.deepcopy.go` | controller-gen | `make codegen` |
| `config/crd/bases/*.yaml` | controller-gen | `make manifests` |

After modifying API types, regenerate and commit the results in the same PR:

```bash
make generate          # runs both codegen and manifests
make verify-manifests  # verify generated files are up to date
```

## Development Quick Reference

| Task | Command |
| ---- | ------- |
| Build all binaries | `make build` |
| Build operator only | `make build-operator` |
| Build interceptor only | `make build-interceptor` |
| Build scaler only | `make build-scaler` |
| Run unit tests | `make test` |
| Run linter | `make lint` |
| Run linter with auto-fix | `make lint-fix` |
| Generate code and manifests | `make generate` |
| Verify manifests are up to date | `make verify-manifests` |
| Run e2e tests | `make e2e-test` |
| Run e2e tests (specific profile) | `make e2e-test PROFILE=tls` |
| Run e2e tests (specific test) | `make e2e-test RUN=TestColdStart` |
| Build e2e test images | `make e2e-test-images` |
| Install e2e dependencies | `make e2e-deps` |
| Deploy local changes to a dev cluster (via ko) | `make deploy` |

## Pre-Submit Checklist

Before requesting review:

1. `make build` — Verify the code compiles
2. `make test` — Run unit tests
3. `make lint` — Run linters
4. `make verify-manifests` — Ensure generated files are up to date
5. Review your diff for secrets, credentials, or debug code
6. Address any [CodeRabbit](https://coderabbit.ai/) review feedback — as a courtesy to the human reviewer who follows.
   Responding with an explanation of why you're not acting on a suggestion is fine; the goal is to resolve straightforward issues so human reviewers can focus on the substantive aspects.

## Code Style

- Run `make lint-fix` before committing — it covers govet, staticcheck, gosec, gofumpt, and more (see `.golangci.yml`)
- Use Go standard library for testing (`testing` package, not testify/gomega/ginkgo) unless there are strong reasons
- Follow Go conventions for error strings: lowercase, no trailing punctuation, wrap with `fmt.Errorf("context: %w", err)`
- In Markdown files, put each sentence on its own line
