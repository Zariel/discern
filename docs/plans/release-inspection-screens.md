## Goal

Build the hosted UI view layer for searchable release and
release-instance inspection so operators can examine canonical
metadata, provenance, and export state through the existing
inspection API.

## Scope

- Add transport-agnostic library search screen models.
- Add release detail screen models with canonical tracks.
- Add release-instance detail screen models with export preview.
- Extend the shared web API path catalog for search and inspection
  routes used by these screens.

## Non-Goals

- Implement candidate review or manual override screens.
- Render HTML or introduce a browser runtime.
- Add new inspection endpoints.

## Affected Modules

- `docs/plans/release-inspection-screens.md`
- `src/web/mod.rs`
- `src/web/client.rs`
- `src/web/inspect.rs`

## Schema Changes

- None.

## API Changes

- None. This task consumes the existing inspection API only.

## State Machine Changes

- None.

## Test Plan

- Library search tests covering release-group search and release list
  loading.
- Release detail tests covering canonical metadata and track loading.
- Release-instance detail tests covering technical/provenance detail
  plus export preview loading.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps the inspection UI at the same replaceable Rust-side view-model
  layer as the issue queue and jobs screens.
