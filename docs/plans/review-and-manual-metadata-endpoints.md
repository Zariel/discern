## Goal

Expose typed operator-resolution handlers for candidate review,
manual match selection, and manual metadata overrides using the
shared REST API envelope.

## Scope

- Add candidate-match list and selection handlers.
- Add explicit manual match resolution by canonical release id.
- Add manual metadata patch handlers for releases, release
  instances, and track instances.
- Add focused API tests for review and override flows.

## Non-Goals

- Add issue queue or issue action endpoints.
- Add HTTP framework wiring.
- Expand override rendering beyond the fields already modeled.

## Affected Modules

- `src/api/mod.rs`
- `src/api/review.rs`
- `src/application/manual_metadata.rs`
- `src/application/matching.rs`
- `src/application/mod.rs`

## Schema Changes

- None.

## API Changes

- Adds candidate review, match resolution, and manual metadata
  override handler surfaces.

## State Machine Changes

- Candidate selection and explicit match resolution move release
  instances into `matched` and resolve open review issues.

## Test Plan

- Candidate list test with pagination.
- Candidate selection test that materializes a canonical release.
- Explicit match resolution test using an existing release id.
- Manual metadata patch tests for release, release instance, and
  track instance subjects.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- Override endpoints persist authoritative operator input without
  leaking repository internals into the API surface, so later
  issue and diagnostics handlers can reuse the same resources.
