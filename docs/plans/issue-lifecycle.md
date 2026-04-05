# ExecPlan: Issue Lifecycle

## Goal
Model issue lifecycle and operator actions so pipeline failures can be
created, resolved, and suppressed as explicit queue items.

## Scope
- Extend the issue domain model with lifecycle metadata and transition
  rules.
- Add an application service for opening, resolving, and suppressing
  issues.
- Add a write-side issue repository trait and SQLite implementation.
- Add tests for domain transitions, application behavior, and SQLite
  persistence.

## Non-Goals
- Job queue implementation.
- API endpoints for issue actions.
- Multi-user audit or RBAC behavior.

## Affected Modules/Files
- `docs/plans/issue-lifecycle.md`
- `src/domain/issue.rs`
- `src/domain/tests.rs`
- `src/application/`
- `src/infrastructure/sqlite.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- Add explicit issue transitions for `open -> resolved` and
  `open -> suppressed`.

## Test Plan
- `cargo test`
- `cargo fmt`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Internal-only lifecycle implementation from an unreleased baseline.
