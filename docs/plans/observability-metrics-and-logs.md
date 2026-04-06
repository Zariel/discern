## Goal

Add in-process observability for the single-process runtime with
Prometheus-style metrics and structured log events covering imports,
jobs, provider calls, duplicates, file operations, issues, and
compatibility failures.

## Scope

- Add a shared observability context with counters, gauges,
  histogram-like duration tracking, and structured event records.
- Expose Prometheus text rendering from the metrics registry for later
  diagnostics and health endpoints.
- Wire the context into runtime bootstrap, application services,
  pipeline execution, ingest flows, and metadata provider clients.
- Instrument the existing workflow paths without adding external
  logging or metrics dependencies.

## Non-Goals

- Add HTTP metrics or log endpoints.
- Add external log sinks or OpenTelemetry exporters.
- Add liveness or readiness endpoints.

## Affected Modules

- `docs/plans/observability-metrics-and-logs.md`
- `src/application/observability.rs`
- `src/application/mod.rs`
- `src/application/services.rs`
- `src/application/ingest.rs`
- `src/application/pipeline.rs`
- `src/infrastructure/mod.rs`
- `src/infrastructure/musicbrainz.rs`
- `src/infrastructure/discogs.rs`
- `src/runtime/mod.rs`

## Schema Changes

- None.

## API Changes

- None in this task. The metrics registry exposes Prometheus text
  internally so the next diagnostics task can surface it.

## State Machine Changes

- None. Instrumentation observes existing job, import, duplicate,
  issue, and compatibility transitions.

## Test Plan

- Unit tests for metric recording, gauge snapshots, histogram output,
  and structured log rendering.
- Runtime bootstrap tests proving shared observability is initialized.
- Pipeline and provider tests asserting key metrics and structured
  events for success and failure paths.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps observability in-process and dependency-light so it can be
  surfaced by the later operational diagnostics task without changing
  instrumentation call sites.
