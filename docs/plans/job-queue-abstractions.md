# ExecPlan: Job Queue Abstractions

## Goal
Define the queued job lifecycle and application queue interfaces so
later worker pools and job APIs can build on explicit persisted job
behavior.

## Scope
- Extend the job domain model with queue and retry transitions.
- Add application services for enqueue, start, complete, fail, retry,
  and recovery actions.
- Add a write-side job repository trait and SQLite implementation.
- Add tests for retry and restart-oriented job semantics.

## Non-Goals
- Full bounded worker-pool execution.
- HTTP job endpoints.
- End-to-end ingest orchestration.

## Affected Modules/Files
- `docs/plans/job-queue-abstractions.md`
- `src/domain/job.rs`
- `src/domain/tests.rs`
- `src/application/`
- `src/infrastructure/sqlite.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- Add explicit job lifecycle transitions for queueing, running,
  succeeding, failing, marking resumable, and retrying.

## Test Plan
- `cargo test`
- `cargo fmt`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only queue abstraction from an unreleased baseline.
