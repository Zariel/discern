## Goal

Persist selected release artwork and export a stable player-friendly
sidecar such as `cover.jpg` alongside organized managed files.

## Scope

- Add release-artwork repository contracts and SQLite support.
- Add an artwork service that selects operator or source-local artwork.
- Copy the chosen artwork into the managed release directory using the
  configured sidecar filename.
- Wire artwork export into the organize stage.
- Add focused application coverage for selected and missing artwork.

## Non-Goals

- Add provider artwork download.
- Add embedded-artwork writing into tags.
- Add artwork-specific API endpoints.

## Affected Modules

- `src/application/artwork.rs`
- `src/application/mod.rs`
- `src/application/pipeline.rs`
- `src/application/repository.rs`
- `src/infrastructure/sqlite.rs`

## Schema Changes

- None. Uses the existing `release_artwork` table in the squashed
  initial schema.

## API Changes

- None.

## State Machine Changes

- Organize stage now exports artwork before verification.
- Missing artwork opens or resolves `missing_artwork` issues for the
  release instance.

## Test Plan

- Artwork export test for operator-selected source-local artwork.
- Missing-artwork issue test when no candidate image exists.
- End-to-end organize pipeline test asserting sidecar output.
- Full validation with `cargo fmt --all`, `cargo test`, and
  `cargo clippy -- -D warnings`

## Rollout Notes

- The implementation stays conservative by using only operator-selected
  and source-local artwork, leaving provider download to later work.
