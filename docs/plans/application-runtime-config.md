# ExecPlan: Application Runtime Config

## Goal
Surface a validated, application-facing runtime config object so
services can consume normalized policy inputs and config diagnostics
without depending on the full bootstrap schema.

## Scope
- Add an application-layer validated runtime config view.
- Normalize service-relevant storage, import, export, provider, and
  worker policy inputs.
- Surface config diagnostics alongside the normalized policy view.
- Wire the validated config into the application context during
  runtime bootstrap.
- Add tests for normalization and runtime wiring.

## Non-Goals
- Config file loading.
- API endpoints for config diagnostics.
- New validation rules beyond the existing startup checks.

## Affected Modules/Files
- `docs/plans/application-runtime-config.md`
- `src/application/mod.rs`
- `src/application/config.rs`
- `src/application/services.rs`
- `src/runtime/mod.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- None.

## Test Plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only application wiring from an unreleased baseline.
