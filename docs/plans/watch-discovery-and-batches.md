# ExecPlan: Watch Discovery and Batch Creation

## Goal
Scan configured watch directories conservatively, create import batches
for newly discovered inputs, and enqueue discovery jobs while
preserving source intent.

## Scope
- Add source and import-batch command repositories.
- Add a watch discovery service that scans configured watch
  directories.
- Reuse or create watch-directory source records.
- Create import batches only for newly discovered supported inputs and
  enqueue `discover_batch` jobs.
- Add unit tests for conservative scanning and rescan idempotency.
- Add SQLite persistence coverage for source and batch writes.

## Non-Goals
- Deep grouping heuristics.
- Metadata extraction.
- API-pushed or manual ingest submission.

## Affected Modules/Files
- `docs/plans/watch-discovery-and-batches.md`
- `src/application/mod.rs`
- `src/application/ingest.rs`
- `src/application/repository.rs`
- `src/infrastructure/sqlite.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- Create `import_batches` in `created` state and queue matching
  `discover_batch` jobs.

## Test Plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal ingest wiring from an unreleased baseline.
