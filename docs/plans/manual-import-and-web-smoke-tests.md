## Goal

Add the hosted UI flow for manual import submission and cover the
core operator journeys with SQLite-backed smoke tests.

## Scope

- Add transport-agnostic manual import actions on top of the existing
  ingest API.
- Add batch listing support for the manual import screen.
- Add end-to-end smoke tests that exercise manual import, job
  inspection, candidate review, and export preview through the web
  layer.

## Non-Goals

- Introduce a browser runtime or HTML rendering.
- Add new ingest, review, or export endpoints.
- Change import, review, or export business rules.

## Affected Modules

- `docs/plans/manual-import-and-web-smoke-tests.md`
- `src/web/import.rs`
- `src/web/mod.rs`
- `tests/web_operator_smoke.rs`

## Schema Changes

- None.

## API Changes

- None. This task consumes existing ingest, jobs, review, and
  inspection APIs only.

## State Machine Changes

- None.

## Test Plan

- Smoke test covering manual import submission plus batch and job
  inspection through the web-layer loaders.
- Smoke test covering candidate review selection plus export preview
  loading through the web-layer loaders against persisted SQLite
  state.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps the hosted UI implementation at the replaceable Rust-side
  view-model layer while proving the operator flows against real
  repositories.
