# ExecPlan: SQLite Contract Tests

## Goal
Verify migration bootstrap and key SQLite repository contracts so later
ingest, issue, and API work can rely on stable schema and query
behavior.

## Scope
- Add migration round-trip tests for schema creation and teardown.
- Add repository contract tests for pagination, filtering, and not-found
  behavior.
- Add focused index presence checks for the key query paths introduced
  in the SQLite repository layer.

## Non-Goals
- New repository features.
- Exhaustive performance benchmarking.
- HTTP or application-service integration tests.

## Affected Modules/Files
- `docs/plans/sqlite-contract-tests.md`
- `src/infrastructure/sqlite.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- None.

## Test Plan
- `cargo test`
- `cargo fmt`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Test-only coverage expansion from an unreleased baseline.
