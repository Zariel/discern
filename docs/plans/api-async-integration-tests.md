## Goal

Add repository-backed API integration tests that exercise async workflow
endpoints, response envelopes, pagination metadata, and core operator
actions across multiple handlers.

## Scope

- Add SQLite-backed integration coverage under `tests/`.
- Verify ingest submission and job inspection/retry flows through the
  public API handlers.
- Verify async candidate selection plus issue inspection and resolution
  against persisted state.

## Non-Goals

- Add HTTP transport wiring.
- Add web UI coverage.
- Expand API surface beyond the existing handlers.

## Affected Modules

- `tests/api_async_workflows.rs`
- `docs/plans/api-async-integration-tests.md`

## Schema Changes

- None. Uses the current SQLite schema through repository APIs.

## API Changes

- None.

## State Machine Changes

- None. Tests verify the current ingest, retry, match-selection, and
  issue-resolution transitions.

## Test Plan

- Ingest submission plus job listing/get coverage with envelope and
  pagination assertions.
- Job retry coverage that verifies queued job state and reset release
  instance state.
- Tokio-backed candidate selection coverage that materializes canonical
  release identity and resolves review issues.
- Issue detail and resolve coverage with export diagnostics.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Uses real SQLite repositories so later API changes can be checked
  without duplicating large in-memory repository doubles.
