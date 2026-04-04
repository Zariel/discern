# ExecPlan: Configuration Schema

## Goal
Model the service configuration surface for import, export, provider,
storage, and worker policy so later validation and runtime loading
work against explicit typed structures.

## Scope
- Expand `AppConfig` to cover watch directories, managed library
  storage, import policy, export profiles, provider settings, worker
  concurrency, and server binding.
- Reuse existing domain enums for shared concepts such as import mode
  and supported audio formats.
- Keep validation focused on schema consistency and lightweight field
  invariants, leaving unsafe topology checks to the next task.
- Add unit tests for default config and schema consistency rules.

## Non-Goals
- File parsing or config loading from disk.
- Startup validation for watcher overlap, provider credential
  requirements, or path distinguishability.
- Export rendering or provider client implementations.

## Affected Modules/Files
- `docs/plans/config-schema.md`
- `src/config/`
- `src/infrastructure/`
- `src/runtime/`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- None.

## Test Plan
- `cargo test`
- `cargo fmt`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only schema expansion from an unreleased baseline.
- Preserves a conservative separation between config modeling and
  higher-risk startup validation.
