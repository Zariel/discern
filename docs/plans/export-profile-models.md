# Goal

Define explicit export-profile and player-visibility policy models that
sit between raw config parsing and later export rendering logic.

# Scope

- add domain types for export profiles, qualifier visibility, artwork
  behavior, and player-facing field selection
- expose normalized export-profile policy data through the application
  config layer
- add domain and application tests covering the default generic-player
  policy and config normalization

# Non-Goals

- rendering player-facing tags
- path rendering
- writing tags to files
- API exposure of export profiles

# Affected Modules

- `src/domain/export_profile.rs`
- `src/domain/mod.rs`
- `src/domain/tests.rs`
- `src/application/config.rs`
- `docs/plans/export-profile-models.md`

# Schema Changes

None.

# API Changes

No external API changes.

# State Machine Changes

None.

# Test Plan

- domain test for the default generic-player export profile
- application config test for normalized export-profile policy mapping
- preserve existing config validation coverage for hidden/path-only
  combinations

# Rollout Notes

This is a modeling slice only. Later rendering work should depend on
these normalized policy types instead of reaching back into raw config
structures.
