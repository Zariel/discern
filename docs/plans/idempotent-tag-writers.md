# Goal

Implement idempotent FLAC and MP3 tag writers that map exported metadata
snapshots onto source audio files for a matched release instance.

# Scope

- add an application tagging service for FLAC and MP3 source files
- add the minimal repository reads needed to fetch canonical tracks,
  staging-manifest file paths, and the latest exported metadata snapshot
- write player-facing album and track tags from canonical/exported state
- preserve unknown tags according to config policy
- make repeated tagging idempotent for the same inputs
- cover repeated-write behavior with application and filesystem-backed
  tests

# Non-Goals

- managed file organization
- artwork embedding
- non-FLAC or non-MP3 formats
- track-instance persistence
- full job-worker orchestration

# Affected Modules

- `src/application/tagging.rs`
- `src/application/mod.rs`
- `src/application/repository.rs`
- `src/domain/track.rs`
- `src/infrastructure/sqlite.rs`
- `docs/plans/idempotent-tag-writers.md`

# Schema Changes

None.

# API Changes

No external API changes.

# State Machine Changes

- successful tagging can advance a matched or export-rendered release
  instance into `tagging` for the duration of the write flow
- the tagging service itself is synchronous and leaves job orchestration to
  a later worker-pool slice

# Test Plan

- application test for canonical field mapping into exported FLAC and MP3
  tags
- application test for unknown-tag preservation policy behavior
- filesystem-backed test that repeated tagging yields the same stored tag
  values for FLAC and MP3 fixtures
- SQLite repository test for canonical track reads used by tagging

# Rollout Notes

This keeps tag output derived from canonical release data plus the latest
export snapshot so manual overrides and export-profile policy remain the
single source of truth for player-facing metadata.
