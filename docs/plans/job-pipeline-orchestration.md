## Goal

Wire persisted jobs to the existing ingest, matching, export, tagging,
organization, and compatibility services so pipeline stages update job
progress, move release instances through the expected states, and enqueue the
next stage automatically.

## Scope

- Add an application job-pipeline orchestrator that dispatches by `JobType`.
- Reuse existing stage services instead of duplicating stage logic.
- Update release-instance state transitions for render, tag, organize, and
  verify stages.
- Enqueue downstream jobs as each stage completes.
- Surface stage failures in job status and open conservative operator-facing
  issues where existing issue types fit clearly.

## Non-Goals

- Add REST endpoints for job execution.
- Implement restart policy beyond existing resumable-job behavior.
- Add duplicate, quarantine, or reprocess flows from later backlog items.
- Introduce background daemon wiring in runtime bootstrap.

## Affected Modules

- `src/application/pipeline.rs`
- `src/application/mod.rs`
- `src/application/jobs.rs`
- `src/application/repository.rs`
- `src/application/ingest.rs`
- `src/application/matching.rs`
- `src/application/tagging.rs`
- `src/application/organize.rs`
- `src/application/compatibility.rs`

## Schema Changes

- None.

## API Changes

- None.

## State Machine Changes

- `discover_batch` drives batch discovery into grouped analysis output and
  queues matching.
- `match_release_instance` materializes matched release instances and queues
  enrichment for confident matches.
- `enrich_release_instance` queues export rendering after Discogs enrichment.
- `render_export_metadata` sets `rendering_export` before rendering and queues
  tagging.
- `write_tags` sets `tagging` before tag writes and queues organization.
- `organize_files` sets `organizing` before file movement and queues
  verification.
- `verify_import` sets `verified` only when compatibility verification passes.

## Test Plan

- Job orchestration test for discover-to-match queueing
- Job orchestration test for match materialization and downstream enrich jobs
- End-to-end release-instance job chain test for render, tag, organize, and
  verify
- Failure test for tag-write errors producing a `broken_tags` issue and failed
  job
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- The orchestrator is conservative about next-stage queueing and issue
  creation. Reviewable match outcomes stop before export, while verification
  failures persist issues without pretending the import is fully verified.
