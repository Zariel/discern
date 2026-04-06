## Goal

Scaffold the hosted operator UI shell and API integration catalog so
later screen tasks can reuse stable routes, navigation structure, and
REST resource paths without redefining them per view.

## Scope

- Add hosted UI route definitions for the main operator screens.
- Add a dense shell model with navigation groups and a default landing
  route.
- Add a web API client catalog that derives resource paths from the
  shared API base path.
- Wire the shell and API client catalog into the existing `WebSurface`.

## Non-Goals

- Render actual HTML templates or static assets.
- Implement issue, review, search, preview, or jobs screens.
- Add browser-side state management beyond catalog and route models.

## Affected Modules

- `docs/plans/web-ui-shell-and-api-client.md`
- `src/web/mod.rs`
- `src/web/shell.rs`
- `src/web/client.rs`
- `src/runtime/mod.rs`

## Schema Changes

- None.

## API Changes

- None to the server API. This task only catalogs existing API paths
  for the hosted UI.

## State Machine Changes

- None.

## Test Plan

- Unit tests for shell route coverage and default landing route.
- Unit tests for API client path derivation from the shared API base.
- Runtime test coverage proving the web surface receives shell and API
  integration metadata.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps the UI scaffold transport-agnostic so later screen tasks can
  plug in actual rendering without changing route or API contracts.
