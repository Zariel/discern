# ExecPlan: SQLite Repositories

## Goal
Implement SQLite-backed repository readers and a shared SQLite context
that configures WAL mode and preserves a conservative single-writer
strategy for later write operations.

## Scope
- Add a shared SQLite context with connection setup and writer mutex.
- Implement SQLite repository types for the existing repository
  traits.
- Add row-mapping helpers for domain IDs, enums, and JSON-backed
  evidence fields.
- Add repository-focused tests using the committed migrations.

## Non-Goals
- Repository write commands beyond connection-level write support.
- Full repository contract coverage for every query edge case.
- Postgres abstractions beyond the existing trait boundary.

## Affected Modules/Files
- `docs/plans/sqlite-repositories.md`
- `Cargo.toml`
- `src/support/ids.rs`
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
- Internal-only persistence implementation from an unreleased baseline.
- WAL mode and the shared writer mutex keep the runtime aligned with
  the TDD's conservative SQLite write topology.
