# AGENTS.md — openshift/kedacore-http-add-on

This file provides AI-specific guidance for working in the OpenShift downstream fork of [KEDA HTTP Add-on](https://github.com/kedacore/http-add-on).
For contribution guidelines, see [CONTRIBUTING_OPENSHIFT.md](./CONTRIBUTING_OPENSHIFT.md).

## Project Overview

This repo contains the KEDA HTTP Add-on for OpenShift, a downstream fork of [kedacore/http-add-on](https://github.com/kedacore/http-add-on).
The HTTP Add-on enables autoscaling HTTP workloads on Kubernetes (including to/from zero) based on incoming HTTP traffic.
It extends [KEDA](https://keda.sh) with HTTP-aware scaling.

The HTTP Add-on is deployed and managed by the [custom-metrics-autoscaler-operator](https://github.com/openshift/custom-metrics-autoscaler-operator) as part of the Custom Metrics Autoscaler (CMA) for OpenShift.

### Three Components

This repo produces three separate Go binaries, each running as its own container:

| Binary | Source | Purpose |
| ------ | ------ | ------- |
| Operator | `operator/` | Kubernetes controller for the `InterceptorRoute` CRD (see [Request Flow](#request-flow) — it currently does little) |
| Interceptor | `interceptor/` | HTTP reverse proxy that routes requests and tracks pending request counts for scaling decisions |
| Scaler | `scaler/` | gRPC service implementing the KEDA external scaler protocol; aggregates queue metrics from interceptors |

### CRDs

| CRD | API Group | Purpose |
| --- | --------- | ------- |
| InterceptorRoute | `http.keda.sh/v1beta1` | Defines routing/cold-start/scaling-metric config; read directly by the interceptor and scaler |

## Repository Structure

```
operator/
  apis/            # CRD types
  controllers/     # Reconciliation logic
interceptor/       # HTTP reverse proxy, request queue tracking
scaler/            # gRPC external scaler for KEDA
pkg/               # Shared libraries (queue, routing, k8s helpers, utilities)
config/            # Kustomize overlays and generated CRD manifests
test/              # E2E tests, organized by profile
hack/              # CI helper scripts
openshift/         # Downstream-only Containerfiles
docs/              # Developer docs
```

## Upstream / Downstream Relationship

This repo currently tracks upstream `kedacore/http-add-on` on the `main` branch; downstream is single-stream and doesn't use release branches today.
If a downstream release ever needs to pin to a specific upstream release point, or if a rebase targets an upstream release branch instead of `main`, that would introduce release branches.

### What is downstream-only

These files exist only in the downstream fork:

- `.ci-operator.yaml` — OpenShift CI build root config
- `.coderabbit.yaml` — CodeRabbit configuration
- `AGENTS.md` — This file
- `CONTRIBUTING_OPENSHIFT.md` — Downstream contributing guide
- `OWNERS` — OpenShift approver list
- `openshift/Containerfile.*.rhel` — OpenShift container image definitions

### What is upstream

Everything else.
In particular: the operator, interceptor, and scaler source code, the API types in `operator/apis/`, the Makefile, the upstream Dockerfiles, and the `.github/` workflows (which are not used in our CI but are kept for upstream compatibility).
Avoid modifying upstream files in this repository since patches will need to be re-applied during every rebase cycle.
Instead, try to contribute features and fixes upstream.

### Rebase Cycle

Periodically, the `main` branch is rebased onto a newer upstream release.
During a rebase:

- `UPSTREAM: <drop>:` commits are discarded
- `UPSTREAM: <carry>:` commits are re-applied
- `UPSTREAM: 1234:` commits are dropped if upstream PR 1234 is now included, otherwise re-applied

## Architecture: What Is Not Obvious

### Request Flow

1. User creates an **InterceptorRoute** for routing/cold-start/scaling-metric config, and separately creates a `keda.sh` **ScaledObject** by hand, with its `external-push` trigger metadata pointing at the scaler and referencing the InterceptorRoute by name
2. The operator's `InterceptorRoute` controller only sets the `Ready` status condition — it does not create or own the ScaledObject
3. The **interceptor** watches `InterceptorRoute` directly (independent of the operator reconcilers) to build its routing table, and acts as a reverse proxy — it receives incoming HTTP requests, tracks pending request counts in an in-memory queue, and forwards requests to the target workload
4. The **scaler** implements KEDA's external scaler gRPC protocol — it reads the `InterceptorRoute` named in the trigger metadata and reports pending request metrics to KEDA
5. KEDA uses these metrics to scale the target workload up or down (including to/from zero)

### Interceptor as Proxy

The interceptor is both a data plane component (proxying HTTP traffic) and a metrics source (tracking pending requests).
This dual role means changes to the interceptor can affect both request routing correctness and scaling accuracy.

### Container Images

`openshift/Containerfile.*.rhel` builds images for CI validation only, via OpenShift CI (ci-operator), not ko.
The actual release artifacts shipped to customers are built separately by Konflux — see [custom-metrics-autoscaler-pipelines](https://github.com/openshift/custom-metrics-autoscaler-pipelines).
[ko](https://ko.build/) is still fine for local development — `make deploy` uses it to push and deploy your changes to a dev cluster.

## Common Pitfalls

1. **Add a carry prefix to every commit.**
   Every non-upstream commit must use `UPSTREAM: <carry>:`, `UPSTREAM: <drop>:`, or `UPSTREAM: 1234:`.
   Only commits merged from upstream don't require the prefix.

2. **Do not hand-edit generated files.**
   CRD manifests in `config/crd/bases/` and files matching `zz_generated*` are generated.
   Run `make generate` instead.

3. **Do not use GitHub Actions.**
   The `.github/workflows/` directory is from upstream and is not used in our CI.
   Our CI runs through OpenShift CI (Prow).
   The CI config is in [openshift/release](https://github.com/openshift/release/tree/master/ci-operator/config/openshift/kedacore-http-add-on).

4. **Do not modify vendor/ directly.**
   Run `go mod tidy && go mod vendor` and commit vendor changes in a separate commit from logic changes.

5. **Write new tests with the standard `testing` package.**
   New test files should use Go's built-in `testing` package, not testify/gomega/ginkgo.

## Human-in-the-Loop Triggers

Stop and consult a human before:

- **Modifying CRD API types** (`operator/apis/`) — API changes have compatibility implications and may require coordinated changes in the CMA operator repo
- **Changing RBAC permissions** — Privilege changes require security review
- **Modifying the upstream/downstream boundary** — Any change to which files are carried vs. upstream
- **Rebase-related decisions** — Whether a carry patch is still needed, whether to drop or keep
- **Changing container image definitions** — Build changes can affect the product payload
- **Changes affecting the interceptor proxy behavior** — Routing changes can break user traffic

## Paired Changes

These files must be updated together:

| If you change... | Also update... |
| ---------------- | -------------- |
| API types in `operator/apis/http/` | Run `make generate` to regenerate DeepCopy methods and CRDs |
| `go.mod` dependencies | Run `go mod vendor` and commit vendor changes separately |
| E2E test profiles | Update relevant profile directory under `test/e2e/` |
| Protobuf definitions | Run `make generate-proto` |

## Further Reading

- [CONTRIBUTING_OPENSHIFT.md](./CONTRIBUTING_OPENSHIFT.md) — Downstream contribution workflow, PR commands, test expectations
- [CONTRIBUTING.md](./CONTRIBUTING.md) — Upstream contribution guidelines
- [docs/developing.md](./docs/developing.md) — Prerequisites, ko workflow, and writing e2e tests
- [CHANGELOG.md](./CHANGELOG.md) — Release changelog
