# Goal

Materialize canonical MusicBrainz release-group and release records for
confident provisional matches, then attach the affected
`release_instance` rows to those canonical identities.

# Scope

- add MusicBrainz release-detail lookup to the matching provider surface
- add repository commands and lookup helpers for artists,
  release_groups, and releases
- materialize canonical rows from the top confident MusicBrainz release
  candidate for a batch
- move linked `release_instance` rows from `analyzed` to `matched`
- add service and SQLite repository coverage

# Non-Goals

- track or track-instance materialization
- manual overrides
- review issue creation beyond existing matching flows
- Discogs-driven canonical identity

# Affected Modules

- `src/application/matching.rs`
- `src/application/repository.rs`
- `src/infrastructure/musicbrainz.rs`
- `src/infrastructure/sqlite.rs`
- `docs/plans/canonical-release-materialization.md`

# Schema Changes

No schema changes. Existing `artists`, `release_groups`, `releases`, and
`release_instances.release_id` columns are reused.

# API Changes

No external API changes.

# State Machine Changes

- only release instances whose candidate set no longer requires review
  are eligible for canonical materialization
- materialized instances gain a `release_id` and transition to `matched`
- reviewable instances remain unchanged

# Test Plan

- matching test for materializing a confident batch candidate into new
  artist, release_group, and release rows
- matching test for reusing an existing canonical release by MusicBrainz
  release id instead of creating duplicates
- SQLite repository test for artist and release-group MusicBrainz lookup
  plus command persistence

# Rollout Notes

This keeps MusicBrainz as the sole canonical identity source. The first
implementation stops at release-level materialization so later work can
add canonical tracks without rewriting release-instance linkage.
