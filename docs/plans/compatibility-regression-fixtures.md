## Goal

Add representative regression fixtures for compatibility, path
stability, and scale-sensitive listing behavior.

## Scope

- Add fixture-backed compatibility summaries for stable and
  collision-prone release-instance outputs.
- Add a managed-path stability regression that checks repeated
  verification leaves exported path identity unchanged.
- Add a scale regression suite that verifies paginated release
  listing remains stable with a representative large-library seed.

## Non-Goals

- Add new service behavior.
- Change compatibility policy or path rendering rules.
- Add benchmarking infrastructure.

## Affected Modules

- `docs/plans/compatibility-regression-fixtures.md`
- `src/application/compatibility.rs`
- `tests/scale_regression.rs`
- `tests/golden/compatibility_managed_path_stability.txt`
- `tests/golden/compatibility_visibility_collision.txt`
- `tests/golden/large_library_release_page.txt`

## Schema Changes

- None.

## API Changes

- None.

## State Machine Changes

- None.

## Test Plan

- Fixture-backed compatibility regression for stable managed paths.
- Fixture-backed compatibility regression for player-visible
  collision behavior.
- SQLite-backed scale regression for paginated release listing.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- These fixtures give later behavior changes a stable regression
  target for operator-visible compatibility outcomes.
