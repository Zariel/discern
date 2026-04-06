## Goal

Build the hosted UI view layer for the issue queue and jobs screen so
operators can inspect unresolved work, review job progress, and drive
resolve, suppress, or retry actions through the existing REST API
contracts.

## Scope

- Add transport-agnostic issue queue screen models and loaders.
- Add transport-agnostic jobs screen models and loaders.
- Wire issue resolve and suppress actions through the view layer.
- Wire job retry actions through the view layer.
- Extend the web API path catalog with action endpoints used by these
  screens.

## Non-Goals

- Render HTML, CSS, or browser-side components.
- Implement release inspection or candidate review screens.
- Add bulk mutation beyond action affordances for individual items.

## Affected Modules

- `docs/plans/issue-queue-and-jobs-screens.md`
- `src/web/mod.rs`
- `src/web/client.rs`
- `src/web/operate.rs`

## Schema Changes

- None.

## API Changes

- None to server endpoints. This task only consumes existing issue and
  jobs APIs.

## State Machine Changes

- None. UI actions delegate to existing issue and recovery services.

## Test Plan

- Issue queue tests covering unresolved listing, detail loading, and
  resolve or suppress actions.
- Jobs screen tests covering running or failed job summaries and retry
  actions.
- API client path tests covering issue and job action endpoints.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps the UI layer dense and operator-focused by assembling explicit
  screen resources from the REST API types already under test.
