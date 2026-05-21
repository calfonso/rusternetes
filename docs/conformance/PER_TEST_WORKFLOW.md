# Per-test conformance workflow

Drive Claude Code workers one test at a time using hydrophone for single-test runs.

## Quick start

```bash
# Re-run one failing test, save per-test artifacts
bash scripts/conformance-single-test.sh '[sig-api-machinery] CustomResourceDefinition resource Conformance Versions for CRD should be exposed [Conformance]'
```

Output: `.rusternetes/volumes/conformance-per-test-runs/<slug>-<timestamp>/`
containing `e2e.log`, `junit_01.xml`, `run.log`, and `per-test/<name>.txt` (one
file per failing testcase).

Flags:

- `--output-dir <dir>` — override output directory.
- `--kubeconfig <path>` — default `${KUBECONFIG:-~/.kube/rusternetes-config}`.
- `--conformance-image <img>` — default `registry.k8s.io/conformance:v1.35.0`.
- `--no-anchor` — treat the positional arg as a raw regex (skip escaping +
  `^…$` anchoring). Useful for `--focus 'should .*provide DNS'`-style globs.

## Why hydrophone

- Single pod, ~5s startup vs sonobuoy's ~30s.
- SIG-Testing maintained, official conformance replacement.
- Native `--focus REGEX` — exact-anchor for one test.
- Parallel-safe with distinct `--output-dir` and `--skip-preflight`.

The script resolves a hydrophone binary in this order: `$PATH` →
`go install sigs.k8s.io/hydrophone@latest` into a tempdir GOBIN → docker
container fallback (`registry.k8s.io/hydrophone:latest`). It bails out with an
install hint if none of those are available.

## Driving Claude Code workers

1. Run the full suite once: `bash scripts/run-conformance.sh`.
2. Split the junit into per-test files via the splitter (Unit 1's deliverable,
   `scripts/conformance-split-junit.sh`; see `docs/CONFORMANCE.md`).
3. For each failing test file under `.rusternetes/volumes/conformance-per-test/`,
   dispatch a Claude Code worker with that file's contents as the input prompt.
   The worker investigates against the matching `docs/conformance/<area>.md`
   matrix.
4. After a worker proposes a fix, verify with
   `bash scripts/conformance-single-test.sh '<test name>'`. Fast feedback — no
   full-suite re-run needed.

## Parallel runs

Different output dirs + `--skip-preflight` lets multiple invocations coexist.
Avoid scheduling more than the cluster can accept simultaneously — each run
spawns a conformance pod in the `conformance` namespace and the api-server +
kubelets must keep up.

## Comparison with full sonobuoy

`scripts/run-conformance.sh` (sonobuoy, certified-conformance, ~441 tests)
remains the gate for release. `conformance-single-test.sh` is for iterative
fixing: small artifact footprint, fast turnaround, one test per invocation.
