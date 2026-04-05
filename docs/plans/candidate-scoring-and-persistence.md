# Goal
Persist transparent MusicBrainz candidate scores for analyzed ingest
results so operators can inspect why a grouped import appears matched,
unmatched, or ambiguous without recomputing provider calls.

# Scope
- Allow `release_instance` records to exist before canonical release
  selection by attaching them to an `import_batch` and making the
  canonical `release_id` optional until later matching work resolves it.
- Add command-side repository support for provisional release-instance
  creation, release-instance updates, and candidate-match replacement.
- Extend the matching service with a conservative scoring pipeline that
  creates or reuses provisional release instances, scores provider
  candidates with explicit evidence notes, and persists candidate rows.
- Move provisional release instances into `needs_review` when matching
  remains ambiguous or too weak after scoring.
- Add focused service and SQLite coverage for provisional release
  instances and candidate persistence.

# Non-goals
- Canonical release-group or release materialization.
- Discogs enrichment.
- Manual override handling.
- HTTP/API exposure.

# Affected modules/files
- `docs/plans/candidate-scoring-and-persistence.md`
- `migrations/`
- `src/domain/release_instance.rs`
- `src/domain/tests.rs`
- `src/application/repository.rs`
- `src/application/matching.rs`
- `src/infrastructure/sqlite.rs`

# Schema changes
- Add `import_batch_id` to `release_instances`.
- Make `release_instances.release_id` nullable so a provisional release
  instance can exist in `analyzed` or `needs_review` before canonical
  identity is chosen.
- Reuse the existing `candidate_matches` table, still keyed to
  `release_instance_id`, now pointing at provisional release instances.

# API changes
- None.

# State machine changes
- Matching persistence may create a provisional release instance in
  `analyzed`.
- Matching persistence moves the provisional release instance to
  `needs_review` when no conservative auto-ready candidate exists.
- This task does not move a release instance to `matched`; later work
  will assign canonical identity and advance the state.

# Test plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`
- Add unit coverage for scoring decisions and provisional instance
  state changes.
- Add SQLite coverage for provisional release-instance persistence and
  candidate replacement.

# Rollout/backward-compatibility notes
- This remains an unreleased schema baseline, so migration edits are
  acceptable.
- Persisted candidate rows become stable input for later Discogs,
  issue-flow, and manual-override tasks.
