# Goal
Build a conservative matching service that turns analyzed batch groups
into MusicBrainz search probes using observed tags, filenames,
directory names, durations, track counts, and YAML evidence.

# Scope
- Add an application-layer matching service and provider trait.
- Derive per-group evidence summaries from persisted staging manifests
  and ingest evidence records.
- Build conservative MusicBrainz release and release-group queries.
- Fetch raw MusicBrainz candidates for each analyzed group.
- Add unit coverage for evidence extraction and query shaping.

# Non-goals
- Candidate scoring.
- Candidate persistence.
- Automatic match selection.
- Issue creation.

# Affected modules/files
- `src/application/mod.rs`
- `src/application/matching.rs`
- `src/infrastructure/musicbrainz.rs`
- `docs/plans/evidence-driven-matching-service.md`

# Schema changes
- None.

# API changes
- None.

# State machine changes
- None in this task. Later tasks will decide `matched` vs
  `needs_review` after scoring and persistence.

# Test plan
- Verify evidence summaries prefer embedded tags without letting YAML
  silently override canonical identity.
- Verify query construction uses the strongest conservative evidence.
- Verify the service fetches release and release-group probes for each
  staged group.

# Rollout/backward-compatibility notes
- The service is additive and only consumes existing batch analysis
  persistence.
- Later tasks can reuse the returned evidence summaries and raw
  provider candidates for scoring and persistence.
