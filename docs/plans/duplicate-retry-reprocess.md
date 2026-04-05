## Goal

Add conservative duplicate handling plus retry and reprocess recovery
flows so matched imports can be flagged or quarantined, failed work can
be requeued from the correct state, and pipeline jobs cover watcher
rescans plus release-instance reprocessing.

## Scope

- Detect day-1 duplicate release instances after canonical matching.
- Apply configured duplicate policy by allowing, flagging, or
  quarantining the matched release instance.
- Add retry-state reset logic for failed or quarantined jobs.
- Wire `reprocess_release_instance` and `rescan_watcher` through the
  existing pipeline.
- Cover duplicate handling and recovery behavior with application tests.

## Non-Goals

- Add exact-audio duplicate detection or checksum lineage.
- Add API endpoints for retry or reprocess operations.
- Add background startup orchestration for resumable jobs.
- Change export rendering, tagging, or organization semantics beyond
  duplicate and recovery gating.

## Affected Modules

- `src/application/duplicates.rs`
- `src/application/recovery.rs`
- `src/application/pipeline.rs`
- `src/application/ingest.rs`
- `src/application/mod.rs`

## Schema Changes

- None.

## API Changes

- None.

## State Machine Changes

- `enrich_release_instance` now evaluates duplicate policy before export.
- Duplicate policy `flag` opens a duplicate issue and continues.
- Duplicate policy `quarantine` opens a duplicate issue, marks the
  release instance and batch as `quarantined`, and stops downstream
  export work.
- Retry scopes reset failed or quarantined release-instance state to the
  earliest safe stage for the requested scope.
- `reprocess_release_instance` resets the release instance to `staged`
  and queues batch analysis again.
- `rescan_watcher` triggers conservative watch discovery for the named
  watcher and queues new discover jobs.

## Test Plan

- Duplicate flag test keeps the pipeline moving and opens an issue.
- Duplicate quarantine test halts downstream work and marks quarantine.
- Retry test resets release-instance and batch state for representative
  scopes.
- Reprocess pipeline test queues analysis from a release-instance job.
- Rescan watcher pipeline test queues discover jobs for matching watch
  sources.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- Duplicate detection is intentionally conservative and limited to the
  v1 scope of the same release imported from different sources with
  close technical alignment.
- Flagged duplicates still rely on later compatibility verification to
  catch path or player-facing indistinguishability.
