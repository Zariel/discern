# ExecPlan: Startup Config Validation

## Goal
Reject unsafe or impossible runtime configurations during bootstrap and
return structured diagnostics that later API work can expose to
operators.

## Scope
- Add a startup validation report that aggregates multiple config
  errors.
- Validate watcher overlap, managed-library overlap, path template
  syntax, edition/source/technical distinguishability, enabled
  provider credentials, and conservative import mode constraints.
- Update runtime bootstrap to fail with the startup validation report.
- Add tests covering invalid startup scenarios.

## Non-Goals
- Config file parsing.
- HTTP endpoints for validation diagnostics.
- Full path rendering or downstream player compatibility checks beyond
  conservative startup rules.

## Affected Modules/Files
- `docs/plans/startup-config-validation.md`
- `src/config/`
- `src/runtime/`

## Schema Changes
- None.

## API Changes
- None, but the validation report is shaped for later API exposure.

## State Machine Changes
- None.

## Test Plan
- `cargo test`
- `cargo fmt`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only validation tightening from an unreleased baseline.
- Default config remains bootable by adding conservative path template
  placeholders for source and technical qualifiers.
