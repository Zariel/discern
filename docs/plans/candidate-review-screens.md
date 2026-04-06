## Goal

Build the hosted UI view layer for candidate review and manual
resolution so operators can compare match candidates, pick a
canonical release, and record overrides through the existing review
API.

## Scope

- Add transport-agnostic candidate review screen models.
- Add review actions for candidate selection and manual release
  resolution.
- Add review actions for release, release-instance, and track
  override submission.
- Extend the shared web API path catalog for review action routes.

## Non-Goals

- Add new REST endpoints.
- Render HTML or introduce a browser runtime.
- Change matching or override domain rules.

## Affected Modules

- `docs/plans/candidate-review-screens.md`
- `src/web/client.rs`
- `src/web/mod.rs`
- `src/web/review.rs`

## Schema Changes

- None.

## API Changes

- None. This task consumes the existing review API only.

## State Machine Changes

- None. Review actions use existing matching and manual override
  transitions.

## Test Plan

- Candidate review screen tests covering candidate listing and
  selected detail state.
- Review action tests covering candidate selection and manual
  release resolution.
- Override tests covering release, release-instance, and track
  manual metadata writes.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps the candidate review UI at the same replaceable Rust-side
  view-model layer as the issue, jobs, and inspection screens.
