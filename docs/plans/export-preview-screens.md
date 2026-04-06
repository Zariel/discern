## Goal

Build the hosted UI view layer for export preview and managed-path
inspection so operators can inspect player-facing metadata, artwork
selection, and rendered managed paths before retrying or finalizing
work.

## Scope

- Add a transport-agnostic export preview screen model.
- Load release-instance detail and export preview state from the
  existing inspection API.
- Present a deterministic managed path string derived from exported
  path components.
- Surface artwork and compatibility summaries for operator review.

## Non-Goals

- Add new REST endpoints.
- Introduce a browser runtime or HTML rendering.
- Change export rendering behavior or path rules.

## Affected Modules

- `docs/plans/export-preview-screens.md`
- `src/web/export_preview.rs`
- `src/web/mod.rs`

## Schema Changes

- None.

## API Changes

- None. This task consumes the existing inspection API only.

## State Machine Changes

- None.

## Test Plan

- Export preview screen tests covering preview loading, managed path
  rendering, and artwork summary state.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`.

## Rollout Notes

- Keeps export preview UI logic in the same replaceable Rust-side
  view-model layer as the rest of the hosted operator screens.
