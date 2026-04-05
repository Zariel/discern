# ExecPlan: Bounded Worker Pools

## Goal
Add async worker pools with separate limits for file I/O, provider
requests, and DB writes so later pipeline stages can run with bounded
concurrency while keeping SQLite write behavior conservative.

## Scope
- Extend worker configuration to describe per-workload concurrency.
- Validate that each workload limit is non-zero and that SQLite keeps a
  single DB write worker.
- Add async worker-pool types to the application layer using Tokio
  semaphores.
- Wire worker pools into the runtime application context.
- Add tests for config validation, runtime wiring, and permit behavior.

## Non-Goals
- Full job-runner orchestration.
- Pipeline stage execution.
- HTTP or web UI changes.

## Affected Modules/Files
- `docs/plans/bounded-worker-pools.md`
- `Cargo.toml`
- `src/config/mod.rs`
- `src/application/mod.rs`
- `src/application/services.rs`
- `src/application/workers.rs`
- `src/runtime/mod.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- None.

## Test Plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only runtime and configuration changes from an unreleased
  baseline.
