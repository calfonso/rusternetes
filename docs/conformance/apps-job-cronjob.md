# [sig-apps] Job + CronJob — scoped conformance coverage

Crate: `crates/controller-manager` · Test file: `tests/conformance_apps_job_cronjob.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios that exercise the
Job and CronJob controllers — `parallelism`, `completions`, `backoffLimit`,
`activeDeadlineSeconds`, `suspend`, and CronJob `schedule`,
`concurrencyPolicy`, `startingDeadlineSeconds`, and history-limit cleanup.
The goal is a sub-second `cargo test` signal that complements the hour-long
Sonobuoy run captured in
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.

Cross-reference: `docs/CONFORMANCE.md` failure bucket
**"apps controllers"** (~3 failures in Round 160) is split across several
units; for Job + CronJob the round was clean (mostly PASS), so every test in
this fragment is mirrored and expected to pass locally.

Unlike the api-server units, this file does **not** spin up the axum router.
The Job and CronJob controllers consume the `Storage` trait directly, so
tests drive `JobController::reconcile_all()` / `CronJobController::reconcile_all()`
against an `Arc<MemoryStorage>`. That matches the prior art in
`crates/controller-manager/tests/{job_controller_test.rs,cronjob_controller_test.rs,job_completion_modes_test.rs}`
and keeps the suite at the level of the responsible component.

## Coverage matrix

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `Job should run a job to completion when tasks succeed` | apps/job.go:53 | PASS | `job_should_run_to_completion_when_tasks_succeed` | mirrored, passing |
| `Job should run a job to completion when tasks sometimes fail and are not locally restarted` | apps/job.go:777 | PASS | `job_should_complete_when_tasks_sometimes_fail_without_local_restart` | mirrored, passing |
| `Job should fail when exceeds active deadline` | apps/job.go:816 | PASS | `job_should_fail_when_exceeds_active_deadline` | mirrored, passing |
| `Job should fail to exceed backoffLimit` | apps/job.go:925 | PASS | `job_should_fail_to_exceed_backoff_limit` | mirrored, passing |
| `Job should not create pods when created in suspend state` | apps/job.go:228 | PASS | `job_should_not_create_pods_when_created_in_suspend_state` | mirrored, passing |
| `Job should delete pods when suspended` | apps/job.go:258 | PASS | `job_should_delete_pods_when_suspended` | mirrored, passing |
| `Job should adopt matching orphans and release non-matching pods` | apps/job.go:872 | PASS | `job_should_respect_parallelism_as_active_upper_bound` | mirrored, passing (scope reduced to parallelism cap; orphan adoption is not yet implemented in the controller) |
| `CronJob should schedule multiple jobs concurrently` | apps/cronjob.go:57 | PASS | `cronjob_should_schedule_multiple_jobs_concurrently_when_allow` | mirrored, passing |
| `CronJob should not schedule jobs when suspended` | apps/cronjob.go:76 | PASS | `cronjob_should_not_schedule_jobs_when_suspended` | mirrored, passing |
| `CronJob should not schedule new jobs when ForbidConcurrent` | apps/cronjob.go:102 | PASS | `cronjob_should_skip_new_jobs_when_forbid_concurrent` | mirrored, passing |
| `CronJob should replace jobs when ReplaceConcurrent` | apps/cronjob.go:134 | PASS | `cronjob_should_replace_jobs_when_replace_concurrent` | mirrored, passing |
| `CronJob should delete successful finished jobs with limit of one successful job` | apps/cronjob.go:276 | PASS | `cronjob_should_delete_successful_finished_jobs_above_history_limit` | mirrored, passing |
| `CronJob should delete failed finished jobs with limit of one job` | apps/cronjob.go:287 | PASS | `cronjob_should_delete_failed_finished_jobs_above_history_limit` | mirrored, passing |
| `CronJob should be able to schedule after more than 100 missed schedule` | apps/cronjob.go:160 | PASS | `cronjob_should_recover_when_many_schedules_missed` | mirrored, passing |
| `CronJob should support CronJob API operations` | apps/cronjob.go:310 | PASS | `cronjob_should_support_cronjob_api_operations` | mirrored, passing |
| `CronJob startingDeadlineSeconds round-trip` | apps/cronjob.go (utils.go) | n/a | `cronjob_should_preserve_starting_deadline_seconds_on_round_trip` | mirrored, passing (defensive coverage — the controller honors the bound through `should_run_now`, but upstream does not expose this as a single Conformance descriptor) |

## Notes on intentional scope reductions

- **Indexed Job, successPolicy, backoffLimitPerIndex** — these are covered
  by `crates/controller-manager/tests/job_completion_modes_test.rs`, which
  was already in tree and mirrors the same upstream sites with deeper
  regression coverage for the unique-index counting bugs. Duplicating them
  here would be churn; the doc fragment is the single source of truth for
  what lives where.
- **Orphan adoption** (`should adopt matching orphans …`) — the upstream
  test exercises a pod-controller-ref reconciliation path that our Job
  controller does not yet implement. The mirrored Rust test focuses on the
  parallelism cap that the upstream test also asserts; the full orphan
  adoption path is tracked separately and will reopen this row when
  implemented.
- **`startingDeadlineSeconds`** — upstream does not gate this with a single
  `[Conformance]` descriptor, but the bound participates in the catch-up
  logic exercised by `cronjob_should_recover_when_many_schedules_missed`.
  We add a small round-trip test so a regression that drops the field on
  CRUD is caught immediately.
