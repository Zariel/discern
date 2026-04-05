## Goal

Expose typed ingest and job API handlers that use the shared envelope
conventions and existing application services for batch submission,
watch rescans, job listing, job lookup, and retry actions.

## Scope

- Add typed ingest request and response DTOs.
- Add typed job request and response DTOs.
- Map ingest and recovery services into API handlers.
- Map repository-backed job listing and lookup into API handlers.
- Add endpoint-focused tests for ingest submission, watcher rescans,
  job listing, and job retry.

## Non-Goals

- Add HTTP framework routing or server integration.
- Implement release, issue, or export-preview endpoints.
- Add authentication, idempotency storage, or transport middleware.

## Affected Modules

- `src/api/mod.rs`
- `src/api/ingest.rs`
- `src/api/jobs.rs`

## Schema Changes

- None.

## API Changes

- Adds typed ingest and job handler surfaces over the shared envelope.

## State Machine Changes

- None beyond invoking the existing ingest and retry flows.

## Test Plan

- API ingest submission test returns source, batch, and job resources.
- API watcher rescan test returns queued discovery jobs.
- API job list test returns paginated job resources.
- API retry test returns updated queued job state.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- The API layer stays transport-agnostic so later HTTP integration can
  reuse the same typed handlers and response contracts.
