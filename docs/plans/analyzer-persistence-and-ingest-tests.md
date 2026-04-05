# Goal
Persist immutable ingest-analysis output for each import batch so later
matching work can consume stored staging manifests, metadata snapshots,
and evidence records instead of transient in-memory data.

# Scope
- Add SQLite schema for staging manifests and ingest evidence records.
- Expose application repository traits for writing and reading analyzer
  output.
- Implement a batch analysis flow that groups submitted paths, extracts
  evidence, persists the manifest and snapshots, and advances batch
  status conservatively.
- Add integration-style tests using the SQLite repositories.

# Non-goals
- MusicBrainz or Discogs matching.
- Release-instance creation.
- HTTP/API surface.

# Affected modules/files
- `migrations/`
- `src/application/repository.rs`
- `src/application/ingest.rs`
- `src/domain/metadata_snapshot.rs`
- `src/infrastructure/sqlite.rs`

# Schema changes
- Add `staging_manifests` keyed by manifest id and batch id.
- Add `ingest_evidence_records` keyed by evidence id and batch id.
- Reuse `metadata_snapshots` for raw embedded-tag and YAML payload
  capture tied to the import batch.

# API changes
- None.

# State machine changes
- Batch analysis can move `import_batches.status` from `created` or
  `discovering` to `grouped` after persistence succeeds.

# Test plan
- SQLite integration test persists a batch analysis result and reads it
  back.
- Service test verifies grouping, evidence, snapshot persistence, and
  grouped batch status.
- Existing unit tests continue to validate parsing helpers.

# Rollout/backward-compatibility notes
- New tables are additive.
- Metadata snapshots use a new `subject_kind` value for import batches,
  so repository code must tolerate older rows without that value.
