# ExecPlan: Pushed and Manual Ingest

## Goal
Support API-pushed and manual-add ingest intake by registering source
intent, creating import batches, and queueing the same discovery job
path used by watch-directory intake.

## Scope
- Extend the ingest service with API and manual submission methods.
- Reuse or create source records for API clients and manual entries.
- Create import batches from submitted paths without assigning release
  identity.
- Queue `discover_batch` jobs for both flows.
- Add tests for source reuse and batch/job creation.

## Non-Goals
- HTTP endpoints.
- Manifest parsing beyond explicit path lists.
- Grouping heuristics or metadata extraction.

## Affected Modules/Files
- `docs/plans/pushed-and-manual-ingest.md`
- `src/application/ingest.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- Create `import_batches` in `created` state and queue matching
  `discover_batch` jobs for API and manual intake.

## Test Plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal ingest service expansion from an unreleased baseline.
