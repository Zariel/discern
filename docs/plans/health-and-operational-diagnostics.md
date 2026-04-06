## Goal

Expose transport-agnostic liveness, readiness, and metrics diagnostics
so operators can verify the runtime is alive, confirm storage access
and schema readiness, and scrape Prometheus text from the shared
observability registry.

## Scope

- Add liveness and readiness handlers to the diagnostics API module.
- Add a Prometheus metrics handler backed by the shared observability
  context.
- Check SQLite writeability, schema initialization, managed-library
  access, and watch-directory accessibility in readiness results.
- Extend the route catalog with health and metrics paths.

## Non-Goals

- Add HTTP framework integration or content-type handling.
- Add dashboards, alerts, or external metrics exporters.
- Add deeper filesystem hardening beyond readiness probes.

## Affected Modules

- `docs/plans/health-and-operational-diagnostics.md`
- `src/api/diagnostics.rs`
- `src/api/mod.rs`
- `src/api/routes.rs`

## Schema Changes

- None.

## API Changes

- Add liveness endpoint resource.
- Add readiness endpoint resource with per-check results.
- Add Prometheus text diagnostics output from the shared metrics
  registry.

## State Machine Changes

- None. Diagnostics observe current runtime state only.

## Test Plan

- Unit tests for liveness, readiness success, and readiness failure.
- Unit test for Prometheus metrics exposure.
- Route-catalog test coverage for health and metrics paths.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Readiness probes are conservative: they fail if required directories
  are missing, unreadable, or unwritable where needed.
- Metrics stay sourced from the shared in-process registry so later
  HTTP transport can expose the same scrape output unchanged.
