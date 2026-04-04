# ExecPlan: Runtime Module Skeleton

## Goal
Establish the top-level module boundaries and bootstrap flow for
configuration, application, infrastructure, API, and web layers so
subsequent tasks can land in the intended architecture.

## Scope
- Add `config`, `infrastructure`, `api`, `web`, and `runtime`
  modules.
- Add a minimal application service context that composes with the
  existing repository traits.
- Replace the placeholder binary startup with a bootstrap path that
  validates config and assembles the runtime.
- Add unit tests covering bootstrap success and conservative startup
  validation.

## Non-Goals
- Full configuration schema or file loading.
- SQLite repository implementations.
- Real HTTP server or hosted web asset serving.
- Background worker execution.

## Affected Modules/Files
- `docs/plans/runtime-skeleton.md`
- `src/lib.rs`
- `src/main.rs`
- `src/application/`
- `src/config/`
- `src/infrastructure/`
- `src/api/`
- `src/web/`
- `src/runtime/`

## Schema Changes
- None.

## API Changes
- None. The API module only exposes internal bootstrap metadata in
  this change.

## State Machine Changes
- None.

## Test Plan
- `cargo test`
- `cargo fmt`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only scaffolding from an unreleased baseline.
- Keeps startup behavior conservative by validating only the minimal
  config invariants introduced in this change.
