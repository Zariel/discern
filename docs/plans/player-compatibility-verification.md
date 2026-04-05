## Goal

Add a compatibility verification pass for organized release instances that
detects distinguishability problems, likely downstream player collisions, and
missing managed outputs. Persist the verification result on the latest export
snapshot and synchronize operator-facing issues.

## Scope

- Add an application compatibility verifier module.
- Verify organized managed outputs against canonical track counts and expected
  artwork outcomes.
- Compare the current export snapshot against sibling release-instance exports
  in the same release group.
- Persist compatibility verification results on the current exported metadata
  snapshot.
- Create, update, and resolve compatibility-related issues for the current
  release instance.

## Non-Goals

- Read tags back from audio files.
- Implement artwork exporting.
- Add job wiring or API endpoints for verification.
- Broaden issue handling beyond the current release instance.

## Affected Modules

- `src/application/compatibility.rs`
- `src/application/mod.rs`
- `src/application/repository.rs`
- `src/application/export.rs`
- `src/application/organize.rs`
- `src/application/tagging.rs`
- `src/infrastructure/sqlite.rs`

## Schema Changes

- None. Reuse `exported_metadata_snapshots.compatibility_*` and `issues`.

## API Changes

- None.

## State Machine Changes

- None in this slice. Verification records results and issues without changing
  release-instance state.

## Test Plan

- Unit tests for:
  - successful verification with no issues
  - path distinguishability conflict
  - player-visible metadata collision
  - missing managed artwork or track outputs
- Repository test for updating exported metadata snapshots in SQLite
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- The verifier is conservative. It only opens issues when persisted export or
  managed-output state is clearly unsafe for coexistence or downstream players.
