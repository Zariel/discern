## Goal

Add derived operational dashboard views for core service health so
operators can inspect imports, issues, duplicates, failed jobs, and
compatibility regressions without an external dashboard runtime.

## Scope

- Add a transport-agnostic dashboard view model over existing
  repositories.
- Surface summary counts for imports, issues, duplicates, failed
  jobs, and compatibility regressions.
- Publish a small catalog of recommended Prometheus queries for the
  same operational slices.

## Non-Goals

- Introduce a browser runtime or rendered charts.
- Add new REST endpoints.
- Replace external dashboards such as Grafana.

## Affected Modules

- `docs/plans/operational-dashboard-views.md`
- `src/web/dashboard.rs`
- `src/web/mod.rs`

## Schema Changes

- None.

## API Changes

- None.

## State Machine Changes

- None.

## Test Plan

- Dashboard view test covering summary counts for open issues,
  duplicate issues, failed jobs, and compatibility regressions.
- Dashboard query-catalog test covering the recommended Prometheus
  queries.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- This keeps the dashboard slice as a replaceable Rust-side derived
  view while still giving operators immediate health summaries.
