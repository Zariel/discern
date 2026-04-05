## Goal

Recover unfinished jobs during runtime bootstrap, ensure a fresh SQLite
startup can initialize schema safely, and add real SQLite tests that
exercise operational restart behavior.

## Scope

- Initialize the SQLite schema during bootstrap when the database is
  empty.
- Recover queued or running jobs into `resumable` state during startup.
- Surface startup recovery in runtime state and summary output.
- Add runtime tests using actual SQLite repositories instead of only
  in-memory fakes.

## Non-Goals

- Start background workers automatically after bootstrap.
- Add API endpoints for startup recovery state.
- Implement policy-driven automatic requeue on restart.

## Affected Modules

- `src/infrastructure/sqlite.rs`
- `src/runtime/mod.rs`

## Schema Changes

- None.

## API Changes

- None.

## State Machine Changes

- Fresh bootstrap now ensures the initial SQLite schema exists.
- Queued and running jobs are marked `resumable` during bootstrap.

## Test Plan

- Bootstrap test with a temp SQLite path ensures schema initialization
  and zero recovered jobs.
- Bootstrap recovery test seeds a running SQLite job and verifies it is
  persisted as `resumable` after startup.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- Startup recovery remains conservative: unfinished work is marked
  resumable rather than automatically resumed.
