# Goal

Render player-facing exported metadata snapshots from canonical release
state, release-instance technical data, export-profile policy, and
manual overrides.

# Scope

- add an application export-rendering service
- add write-side repository support for exported metadata snapshots
- render stable album title, album artist, artist credits, path
  components, artwork behavior, and basic compatibility warnings
- honor release and release-instance manual overrides for title,
  album-artist, release date, edition qualifier, and artwork selection
- cover rendering and persistence with application and SQLite tests

# Non-Goals

- writing FLAC or MP3 tags
- managed file organization
- full player-compatibility issue generation
- track-level tag rendering

# Affected Modules

- `src/application/export.rs`
- `src/application/mod.rs`
- `src/application/repository.rs`
- `src/domain/exported_metadata_snapshot.rs`
- `src/infrastructure/sqlite.rs`
- `docs/plans/export-metadata-rendering.md`

# Schema Changes

None.

# API Changes

No external API changes.

# State Machine Changes

- rendering an exported snapshot is additive persistence only
- the snapshot records the policy outcome used for later tagging and
  compatibility verification

# Test Plan

- application test for generic-player rendering with edition and
  technical qualifier policy
- application test for manual override precedence in rendered output
- SQLite repository test for persisted exported snapshot writes

# Rollout Notes

This keeps exported metadata as a derived projection from canonical and
operator-approved state, which preserves separation from internal
matching and provenance records.
