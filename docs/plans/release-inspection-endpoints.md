## Goal

Expose typed inspection handlers for releases, release instances,
export previews, and release-group search using the shared REST API
conventions and existing repository queries.

## Scope

- Add release list and detail handlers.
- Add release-instance list and detail handlers.
- Add export-preview read handler for release instances.
- Add release-group search handler for related lookup workflows.
- Add endpoint-focused tests for list, detail, preview, and search.

## Non-Goals

- Add candidate selection, manual metadata mutation, or issue APIs.
- Add transport or HTTP framework wiring.
- Add new persistence queries beyond current repository contracts.

## Affected Modules

- `src/api/mod.rs`
- `src/api/inspection.rs`

## Schema Changes

- None.

## API Changes

- Adds release and release-instance inspection handler surfaces.

## State Machine Changes

- None.

## Test Plan

- Release list test with pagination.
- Release detail test including track payloads.
- Release-instance detail test including export preview summary.
- Release-group search test using text filters.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- Inspection responses stay aligned with existing domain queries so the
  next review and mutation API tasks can layer on top without changing
  resource identity or envelope conventions.
