## Goal

Harden managed file mutations so organize and artwork export validate
their roots, reject unsafe symlinks, prevent traversal outside the
managed library, and use atomic write patterns where copying is
required.

## Scope

- Add shared filesystem-safety helpers for managed writes.
- Enforce managed-root validation and target-parent validation.
- Reject symlinked source files and symlinked managed-path components.
- Use temporary-file or temporary-link plus rename patterns for copy
  and hardlink operations where possible.
- Apply the helpers to organize and artwork export flows.

## Non-Goals

- Rework tagging behavior or source-file ingest analysis.
- Add configurable symlink policies.
- Add cross-platform filesystem abstraction beyond current Rust stdlib
  behavior.

## Affected Modules

- `docs/plans/filesystem-safety-hardening.md`
- `src/application/filesystem.rs`
- `src/application/mod.rs`
- `src/application/organize.rs`
- `src/application/artwork.rs`

## Schema Changes

- None.

## API Changes

- None.

## State Machine Changes

- None. File mutations fail earlier with storage or conflict errors
  instead of attempting unsafe writes.

## Test Plan

- Organize test coverage for symlinked managed roots and symlinked
  source files.
- Artwork export test coverage for symlinked artwork sources.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Readiness already checks accessible roots, but this task enforces
  the same safety expectations at mutation time.
- Atomic rename is used where possible for managed-library copies so
  partial destination files are not left behind on success paths.
