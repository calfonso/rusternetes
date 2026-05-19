# ARC runner scale sets

This directory contains the Helm values and the custom runner image
([Dockerfile](Dockerfile)) used for GitHub Actions self-hosted runners
on the Ukrinasoft NGPC cluster (`ukrinasoft-jones-cloudspace`).

Three scale sets, all in the `arc-runners` namespace:

| Release                         | Tier         | Use                  | Mode         | Resources       |
| ------------------------------- | ------------ | -------------------- | ------------ | --------------- |
| `arc-runner-set`                | large        | `cargo nextest`      | default      | 8Gi / 2cpu      |
| `arc-runner-set-small`          | small        | `cargo clippy`, fmt  | default      | 512Mi / 500m    |
| `arc-runner-set-conformance`    | conformance  | hydrophone, sonobuoy | **dind**     | 12Gi / 3cpu     |

The conformance tier is separate from the others because:

* It needs a Docker daemon inside the pod (`docker compose up` for the
  rusternetes cluster under test). DinD is enabled here only; the cargo
  tiers don't pay the ~3–4 GiB + privileged sidecar overhead.
* Conformance jobs are slow (~5–10 min minimum). Bounded to `maxRunners: 1`
  so concurrent jobs don't fight over `localhost:6443` (the dind sidecar
  and the runner share the pod network namespace).

## Workflow → release mapping

The `runs-on:` label in a workflow file matches the helm release name.

```yaml
runs-on: arc-runner-set              # cargo nextest
runs-on: arc-runner-set-small        # clippy, fmt
runs-on: arc-runner-set-conformance  # hydrophone (.github/workflows/conformance-canary.yml)
```

## First-time install of the conformance tier

The conformance scale set needs to exist on the cluster before any
workflow with `runs-on: arc-runner-set-conformance` can schedule. Run
this once:

```bash
helm upgrade --install arc-runner-set-conformance \
  --namespace arc-runners \
  --version 0.14.1 \
  --set githubConfigSecret=arc-runner-set-gha-rs-github-secret \
  --values ci/arc-runner/values-conformance.yaml \
  --kube-context ukrinasoft-jones-cloudspace \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set
```

Re-uses the existing `arc-runner-set-gha-rs-github-secret` Secret —
the GitHub PAT is not repassed here.

The runner pod will include a privileged `docker:dind` sidecar (the
chart's standard DinD template). If NGPC tenant policy rejects
privileged pods in `arc-runners`, the listener pod will fail to create
and the workflow will sit `queued` forever — kubectl-describe the
listener replica set to see the admission webhook denial.

Updating values:

```bash
helm --kube-context ukrinasoft-jones-cloudspace -n arc-runners upgrade \
  arc-runner-set-conformance --reuse-values --version 0.14.1 \
  --values ci/arc-runner/values-conformance.yaml \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set
```

## Why DinD and not Kubernetes container mode

`containerMode.type: kubernetes` would avoid the privileged sidecar,
but spawns each workflow step as its own pod and provides no Docker
daemon. `docker compose up` (the rusternetes cluster bring-up) does
not work in that mode. Rewriting rusternetes to ship a set of K8s
Deployments instead of compose services would be a separate
multi-week refactor; DinD is the pragmatic path until then.

## Build cache + image pull

The conformance image (`registry.k8s.io/conformance:v1.32.0`, ~270 MiB)
and the rusternetes layer set get pulled fresh on each cold runner.
First runs take ~5 minutes longer than steady-state. Baking the
conformance image into the DinD daemon at startup is a known follow-up.
