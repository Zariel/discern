# Goal
Add a bounded async MusicBrainz client adapter that can search and look
up canonical release metadata while respecting MusicBrainz request
rules and minimizing repeated network work.

# Scope
- Add a typed infrastructure client for MusicBrainz search and lookup.
- Honor configured per-application request rate limits.
- Attach a meaningful User-Agent using the configured contact email
  when available.
- Add a small in-memory response cache keyed by normalized request.
- Cover request shaping, caching, rate limiting, and JSON mapping with
  mocked responses.

# Non-goals
- Match scoring.
- Candidate persistence.
- Discogs support.
- Runtime wiring into the full ingest pipeline.

# Affected modules/files
- `Cargo.toml`
- `src/infrastructure/mod.rs`
- `src/infrastructure/musicbrainz.rs`
- `docs/plans/musicbrainz-client.md`

# Schema changes
- None.

# API changes
- None.

# State machine changes
- None.

# Test plan
- Verify release search request parameters and JSON parsing.
- Verify release-group search request parameters and JSON parsing.
- Verify release lookup includes the expected `inc` parameters.
- Verify identical requests are served from cache.
- Verify concurrent requests respect the configured interval.

# Rollout/backward-compatibility notes
- The client is additive and not yet wired into matching decisions.
- Rate limiting defaults to the configured MusicBrainz policy and can
  be tightened later without changing the public shape.
