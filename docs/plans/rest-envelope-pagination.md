## Goal

Establish the REST envelope, error model, pagination metadata, and
route catalog conventions that later API endpoint tasks will build on.

## Scope

- Add JSON envelope types with `data`, `error`, and `meta`.
- Add stable API error codes and details payload support.
- Add offset-based pagination metadata derived from existing page
  helpers.
- Add a route catalog for the TDD core resources and key endpoints.
- Cover the conventions with serialization and route tests.

## Non-Goals

- Implement HTTP handlers or transport wiring.
- Add resource-specific request or response bodies beyond shared API
  scaffolding.
- Add cursor pagination before real endpoint pressure requires it.

## Affected Modules

- `src/api/mod.rs`
- `src/api/envelope.rs`
- `src/api/error.rs`
- `src/api/pagination.rs`
- `src/api/routes.rs`

## Schema Changes

- None.

## API Changes

- Introduces the shared API response envelope and route definitions.

## State Machine Changes

- None.

## Test Plan

- Envelope serialization test for success responses.
- Error envelope serialization test with explicit code and details.
- Pagination metadata test from offset-based page input.
- Route catalog test covering the core TDD endpoints.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- Pagination uses explicit offset metadata for SQLite simplicity in v1,
  while leaving room for cursor fields in later API tasks.
