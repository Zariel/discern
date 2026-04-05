# Goal

Implement deterministic managed path rendering and safe file
organization for matched release instances.

# Scope

- add an application organizer service that renders managed release and
  track paths from export snapshots, import mode, and path-template
  policy
- support copy, move, and hardlink organization modes for day-one FLAC
  and MP3 inputs
- detect final-path collisions before mutating the managed library root
- verify organized outputs and persist managed file records needed for
  later verification and duplicate handling
- cover coexistence, collision, and mode behavior with application and
  SQLite tests

# Non-Goals

- player-visibility compatibility issue generation
- artwork copying beyond predictable sidecar naming inputs already
  represented in exported metadata
- symlink support
- non-audio file transcoding or transformation

# Affected Modules

- `src/application/organize.rs`
- `src/application/mod.rs`
- `src/application/repository.rs`
- `src/infrastructure/sqlite.rs`
- `docs/plans/managed-path-rendering-and-organization.md`

# Schema Changes

None. Reuse the existing `track_instances` and `files` tables.

# API Changes

No external API changes.

# State Machine Changes

- organizing a release instance writes managed files after tagging
- successful organization can move the release instance toward
  `imported`, while collision or verification failure remains a later
  issue-handling concern

# Test Plan

- application test for deterministic release and track path rendering
- application test for copy, move, and hardlink organization behavior
- application test for collision detection before mutation
- SQLite repository test for track-instance and managed-file
  persistence/lookup needed by the organizer

# Rollout Notes

This keeps final library paths derived from canonical/exported state so
reprocessing stays stable and coexistence cases remain explicit instead
of implicit in source-folder structure.
