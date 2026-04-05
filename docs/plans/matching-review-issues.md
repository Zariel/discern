# Goal

Turn low-confidence or conflicting release matches into explicit review
issues attached to the affected `release_instance` records.

# Scope

- classify persisted candidate sets as unmatched or ambiguous
- open and resolve review issues during batch scoring
- add subject-aware issue lookup support in repositories
- cover the new flow with matching and SQLite tests

# Non-Goals

- manual override workflows
- canonical release materialization
- API or web review surfaces
- reopening suppressed issues automatically

# Affected Modules

- `src/application/matching.rs`
- `src/application/repository.rs`
- `src/infrastructure/sqlite.rs`
- `docs/plans/matching-review-issues.md`

# Schema Changes

No schema changes. Existing `issues` rows and issue types are reused.

# API Changes

No external API changes.

# State Machine Changes

- when batch scoring leaves a `release_instance` in `needs_review`, open
  either `unmatched_release` or `ambiguous_release_match`
- when later scoring no longer requires review, resolve any open issues of
  those types for the same `release_instance`
- suppressed issues remain suppressed and are not reopened automatically

# Test Plan

- matching test for ambiguous candidates opening an ambiguous issue
- matching test for weak or missing candidates opening an unmatched issue
- matching test for rescoring a reviewed instance and resolving the stale
  open issue
- SQLite repository test for filtering issues by subject and type

# Rollout Notes

This is additive. Existing issue rows remain valid and future manual-review
work can build on the subject-aware lookup surface.
