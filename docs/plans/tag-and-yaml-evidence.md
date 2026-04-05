# ExecPlan: Tag and YAML Evidence

## Goal
Extract day-1 embedded tag evidence from FLAC and MP3 inputs and parse
Gazelle-origin YAML as supporting provenance-only evidence for later
matching and review flows.

## Scope
- Add audio tag extraction helpers for MP3 and FLAC inputs.
- Map extracted values into ingest evidence records.
- Parse nearby Gazelle YAML files into supporting evidence records.
- Keep YAML-derived values non-canonical and batch-scoped.
- Add focused tests for evidence mapping and YAML parsing.

## Non-Goals
- Persisting analyzer output.
- Full staging manifest persistence.
- MusicBrainz or Discogs matching.

## Affected Modules/Files
- `docs/plans/tag-and-yaml-evidence.md`
- `Cargo.toml`
- `Cargo.lock`
- `src/application/ingest.rs`

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
- Internal analyzer helpers from an unreleased baseline.
