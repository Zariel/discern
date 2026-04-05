# Goal

Persist operator release-match overrides separately from provider data and
keep those overrides authoritative across rescoring and review flows.

# Scope

- add a manual override field for canonical release selection
- add repository traits and SQLite persistence for manual overrides
- add matching-service support to apply a manual release override
- honor the latest manual release override during rescoring so provider
  candidates and issues do not overwrite operator intent
- cover unmatched and ambiguous review flows with tests

# Non-Goals

- full manual metadata editing UI
- track mapping overrides
- export metadata rendering from overrides

# Affected Modules

- `src/domain/manual_override.rs`
- `src/application/repository.rs`
- `src/application/matching.rs`
- `src/infrastructure/sqlite.rs`
- `migrations/0004_manual_release_match_override.up.sql`
- `migrations/0004_manual_release_match_override.down.sql`
- `docs/plans/manual-override-precedence.md`

# Schema Changes

- extend `manual_overrides.field` to allow a release-match override value
- reuse the existing `manual_overrides` table rather than inventing a
  second operator-intent store

# API Changes

No external API changes.

# State Machine Changes

- applying a manual release-match override links the release instance to
  the selected release and moves it to `matched`
- rescoring respects the latest release-match override and does not move
  the instance back into `needs_review`
- open unmatched or ambiguous issues for that release instance are
  resolved when a manual release-match override is applied

# Test Plan

- SQLite repository test for persisting and listing release-match
  overrides
- matching test where a manual override resolves an unmatched flow and
  survives rescoring
- matching test where a manual override resolves an ambiguous flow and
  survives rescoring

# Rollout Notes

This keeps operator intent separate from provider payloads and candidate
scores so later review APIs can expose both without losing provenance.
