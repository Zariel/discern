## Goal

Expose typed issue queue handlers, issue actions, and config-validation
diagnostics for the operator UI using the shared REST API envelope.

## Scope

- Add issue list and detail handlers.
- Add issue resolve and suppress handlers.
- Add config-validation diagnostics handler from validated runtime config.
- Include current export-preview compatibility details on release-instance
  issue detail responses when available.

## Non-Goals

- Add new export-preview routes beyond the existing inspection endpoint.
- Add dashboard or aggregate metrics endpoints.
- Add HTTP framework wiring.

## Affected Modules

- `src/api/mod.rs`
- `src/api/issues.rs`
- `src/api/diagnostics.rs`
- `src/application/issues.rs`

## Schema Changes

- None.

## API Changes

- Adds issue queue reads and actions.
- Adds config-validation diagnostics output.

## State Machine Changes

- Issue resolve and suppress actions move issues through their existing
  lifecycle states.

## Test Plan

- Issue list test with pagination.
- Issue detail test with export compatibility summary.
- Resolve and suppress action tests.
- Config-validation diagnostics test.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- Issue detail responses reuse persisted export snapshots instead of
  recomputing previews, keeping diagnostics aligned with the last
  verified managed output.
