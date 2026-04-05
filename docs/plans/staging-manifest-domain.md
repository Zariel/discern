# ExecPlan: Staging Manifest Domain

## Goal
Model immutable staging manifests and ingest evidence records so one
ingestion event can persist source provenance and analyzer observations
before canonical release identity is assigned.

## Scope
- Add domain types for staging manifests, discovered files, grouping
  decisions, and related auxiliary files.
- Add domain types for analyzer evidence extracted from tags, file
  names, directory structure, and Gazelle-origin YAML.
- Add unit tests that keep staging records separate from canonical
  release and release-instance identity.

## Non-Goals
- Directory scanning.
- Manifest persistence.
- Analyzer services or YAML parsing.

## Affected Modules/Files
- `docs/plans/staging-manifest-domain.md`
- `src/domain/mod.rs`
- `src/domain/staging_manifest.rs`
- `src/domain/ingest_evidence.rs`
- `src/support/ids.rs`
- `src/domain/tests.rs`

## Schema Changes
- None.

## API Changes
- None.

## State Machine Changes
- None.

## Test Plan
- `cargo fmt --all`
- `cargo test`
- `cargo clippy -- -D warnings`

## Rollout / Backward Compatibility
- Domain-only additions from an unreleased baseline.
