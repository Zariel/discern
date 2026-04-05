# Goal
Add Discogs as a secondary metadata provider for edition
 disambiguation so persisted MusicBrainz candidate sets can gain
 label, catalog, and packaging clues without replacing canonical
 MusicBrainz identity by default.

# Scope
- Add a bounded async Discogs client and provider adapter.
- Extend ingest evidence to capture label and catalog hints from local
  tags and Gazelle-origin YAML when available.
- Extend the matching service with a Discogs enrichment flow that reads
  provisional release instances, local evidence, and existing candidate
  matches, then queries Discogs for release-level enrichment.
- Persist raw Discogs payloads as metadata snapshots and persist
  Discogs candidate rows separately from MusicBrainz candidates.
- Return operator-facing field-difference summaries for label,
  catalog, country, year, and packaging/source clues.
- Keep MusicBrainz as the canonical identity source; Discogs may only
  add confidence modifiers and reviewable evidence.

# Non-goals
- Canonical release materialization.
- Automatic override of MusicBrainz identity.
- API/UI endpoints.
- Artwork download or storage.

# Affected modules/files
- `docs/plans/discogs-enrichment.md`
- `src/domain/ingest_evidence.rs`
- `src/application/ingest.rs`
- `src/application/repository.rs`
- `src/application/matching.rs`
- `src/infrastructure/discogs.rs`
- `src/infrastructure/mod.rs`
- `src/runtime/mod.rs`
- `src/infrastructure/sqlite.rs`

# Schema changes
- None. Reuse `candidate_matches` for Discogs candidate rows and
  `metadata_snapshots` with `discogs_payload` for raw enrichment
  evidence.

# API changes
- None.

# State machine changes
- Discogs enrichment does not change canonical identity.
- It may add Discogs candidate rows and operator-visible differences
  while leaving the release instance in `analyzed` or `needs_review`.

# Test plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`
- Add mocked Discogs client coverage for request shaping and payload
  decoding.
- Add service tests proving Discogs evidence is persisted separately
  and does not silently override MusicBrainz-driven identity.

# Rollout/backward-compatibility notes
- This is additive on top of persisted candidate scoring.
- Later canonical-match materialization can consume the stored Discogs
  evidence without requerying the provider.
