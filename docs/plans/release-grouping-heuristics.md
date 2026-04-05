# ExecPlan: Release Grouping Heuristics

## Goal
Group raw file drops into conservative candidate release inputs using
directory structure and filename continuity without guessing across
ambiguous layouts.

## Scope
- Add ingest grouping helpers that inspect submitted paths.
- Group directory drops by common parent and include nearby auxiliary
  files.
- Group loose files only when track numbering is contiguous within one
  directory.
- Split ambiguous loose-file layouts into separate candidate groups.
- Add unit tests for directory, contiguous loose-file, and ambiguous
  layouts.

## Non-Goals
- Embedded tag parsing.
- Gazelle YAML parsing.
- Staging manifest persistence.

## Affected Modules/Files
- `docs/plans/release-grouping-heuristics.md`
- `src/application/ingest.rs`

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
- Internal ingest heuristic expansion from an unreleased baseline.
