PRAGMA foreign_keys = ON;

CREATE TABLE artists (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sort_name TEXT,
    musicbrainz_artist_id TEXT UNIQUE
);

CREATE TABLE release_groups (
    id TEXT PRIMARY KEY,
    primary_artist_id TEXT NOT NULL REFERENCES artists(id),
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('album', 'ep', 'single', 'live', 'compilation', 'soundtrack', 'other')),
    musicbrainz_release_group_id TEXT UNIQUE
);

CREATE TABLE releases (
    id TEXT PRIMARY KEY,
    release_group_id TEXT NOT NULL REFERENCES release_groups(id),
    primary_artist_id TEXT NOT NULL REFERENCES artists(id),
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    musicbrainz_release_id TEXT UNIQUE,
    discogs_release_id INTEGER UNIQUE,
    edition_title TEXT,
    disambiguation TEXT,
    country TEXT,
    label TEXT,
    catalog_number TEXT,
    release_year INTEGER,
    release_month INTEGER,
    release_day INTEGER,
    CHECK (release_month IS NULL OR (release_month BETWEEN 1 AND 12)),
    CHECK (release_day IS NULL OR (release_day BETWEEN 1 AND 31))
);

CREATE TABLE sources (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('watch_directory', 'api_client', 'manual_add', 'gazelle')),
    display_name TEXT NOT NULL,
    locator_kind TEXT NOT NULL CHECK (locator_kind IN ('filesystem_path', 'api_client', 'manual_entry', 'tracker_ref')),
    locator_value TEXT NOT NULL,
    external_reference TEXT
);

CREATE TABLE import_batches (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    mode TEXT NOT NULL CHECK (mode IN ('copy', 'move', 'hardlink')),
    status TEXT NOT NULL CHECK (status IN ('created', 'discovering', 'grouped', 'submitted', 'quarantined', 'failed')),
    requested_by_kind TEXT NOT NULL CHECK (requested_by_kind IN ('system', 'operator', 'external_client')),
    requested_by_name TEXT,
    created_at_unix_seconds INTEGER NOT NULL
);

CREATE TABLE import_batch_paths (
    import_batch_id TEXT NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (import_batch_id, ordinal)
);

CREATE TABLE release_instances (
    id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES releases(id),
    source_id TEXT NOT NULL REFERENCES sources(id),
    state TEXT NOT NULL CHECK (
        state IN (
            'discovered',
            'staged',
            'analyzed',
            'matched',
            'needs_review',
            'rendering_export',
            'tagging',
            'organizing',
            'imported',
            'verified',
            'quarantined',
            'failed'
        )
    ),
    format_family TEXT NOT NULL CHECK (format_family IN ('flac', 'mp3')),
    bitrate_mode TEXT NOT NULL CHECK (bitrate_mode IN ('constant', 'variable', 'lossless')),
    bitrate_kbps INTEGER,
    sample_rate_hz INTEGER,
    bit_depth INTEGER,
    track_count INTEGER NOT NULL,
    total_duration_seconds INTEGER NOT NULL,
    ingest_origin TEXT NOT NULL CHECK (ingest_origin IN ('watch_directory', 'api_push', 'manual_add')),
    import_mode TEXT CHECK (import_mode IN ('copy', 'move', 'hardlink')),
    duplicate_status TEXT,
    export_visibility_policy TEXT,
    original_source_path TEXT NOT NULL,
    imported_at_unix_seconds INTEGER NOT NULL,
    gazelle_tracker TEXT,
    gazelle_torrent_id TEXT,
    gazelle_release_group_id TEXT
);

CREATE TABLE tracks (
    id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    disc_number INTEGER NOT NULL,
    track_number INTEGER NOT NULL,
    title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    musicbrainz_track_id TEXT UNIQUE,
    duration_ms INTEGER,
    UNIQUE (release_id, disc_number, track_number)
);

CREATE TABLE track_instances (
    id TEXT PRIMARY KEY,
    release_instance_id TEXT NOT NULL REFERENCES release_instances(id) ON DELETE CASCADE,
    track_id TEXT NOT NULL REFERENCES tracks(id),
    observed_disc_number INTEGER NOT NULL,
    observed_track_number INTEGER NOT NULL,
    observed_title TEXT,
    format_family TEXT NOT NULL CHECK (format_family IN ('flac', 'mp3')),
    duration_ms INTEGER,
    bitrate_kbps INTEGER,
    sample_rate_hz INTEGER,
    bit_depth INTEGER,
    UNIQUE (release_instance_id, track_id),
    UNIQUE (release_instance_id, observed_disc_number, observed_track_number)
);

CREATE TABLE files (
    id TEXT PRIMARY KEY,
    track_instance_id TEXT NOT NULL REFERENCES track_instances(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('source', 'managed')),
    format_family TEXT NOT NULL CHECK (format_family IN ('flac', 'mp3')),
    path TEXT NOT NULL,
    checksum TEXT,
    size_bytes INTEGER NOT NULL
);

CREATE TABLE metadata_snapshots (
    id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('release_instance', 'file')),
    subject_id TEXT NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN (
            'embedded_tags',
            'filename_heuristics',
            'directory_structure',
            'gazelle_yaml',
            'musicbrainz_payload',
            'discogs_payload'
        )
    ),
    format TEXT NOT NULL CHECK (format IN ('json', 'yaml', 'text')),
    payload TEXT NOT NULL,
    captured_at_unix_seconds INTEGER NOT NULL
);

CREATE TABLE candidate_matches (
    id TEXT PRIMARY KEY,
    release_instance_id TEXT NOT NULL REFERENCES release_instances(id) ON DELETE CASCADE,
    provider TEXT NOT NULL CHECK (provider IN ('musicbrainz', 'discogs')),
    candidate_kind TEXT NOT NULL CHECK (candidate_kind IN ('release', 'release_group')),
    provider_entity_id TEXT NOT NULL,
    normalized_score REAL NOT NULL,
    evidence_matches_json TEXT NOT NULL,
    mismatches_json TEXT NOT NULL,
    unresolved_ambiguities_json TEXT NOT NULL,
    provider_provenance_json TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL
);

CREATE TABLE exported_metadata_snapshots (
    id TEXT PRIMARY KEY,
    release_instance_id TEXT NOT NULL REFERENCES release_instances(id) ON DELETE CASCADE,
    export_profile TEXT NOT NULL,
    album_title TEXT NOT NULL,
    album_artist TEXT NOT NULL,
    artist_credits_json TEXT NOT NULL,
    edition_visibility TEXT NOT NULL CHECK (edition_visibility IN ('hidden', 'path_only', 'tags_and_path')),
    technical_visibility TEXT NOT NULL CHECK (technical_visibility IN ('hidden', 'path_only', 'tags_and_path')),
    path_components_json TEXT NOT NULL,
    primary_artwork_filename TEXT,
    compatibility_verified INTEGER NOT NULL CHECK (compatibility_verified IN (0, 1)),
    compatibility_warnings_json TEXT NOT NULL,
    rendered_at_unix_seconds INTEGER NOT NULL
);

CREATE TABLE issues (
    id TEXT PRIMARY KEY,
    issue_type TEXT NOT NULL CHECK (
        issue_type IN (
            'unmatched_release',
            'ambiguous_release_match',
            'conflicting_metadata',
            'inconsistent_track_count',
            'missing_tracks',
            'corrupt_file',
            'unsupported_format',
            'duplicate_release_instance',
            'undistinguishable_release_instance',
            'player_visibility_collision',
            'missing_artwork',
            'broken_tags',
            'multi_disc_ambiguity',
            'compilation_artist_ambiguity',
            'player_compatibility_failure'
        )
    ),
    state TEXT NOT NULL CHECK (state IN ('open', 'resolved', 'suppressed')),
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('release', 'release_instance', 'track_instance', 'library')),
    subject_id TEXT,
    summary TEXT NOT NULL,
    details TEXT,
    created_at_unix_seconds INTEGER NOT NULL,
    resolved_at_unix_seconds INTEGER,
    suppressed_reason TEXT
);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL CHECK (
        job_type IN (
            'discover_batch',
            'analyze_release_instance',
            'match_release_instance',
            'enrich_release_instance',
            'render_export_metadata',
            'write_tags',
            'organize_files',
            'verify_import',
            'reprocess_release_instance',
            'rescan_watcher'
        )
    ),
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('import_batch', 'release_instance', 'source_scan')),
    subject_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'resumable')),
    progress_phase TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    triggered_by TEXT NOT NULL CHECK (triggered_by IN ('system', 'operator')),
    created_at_unix_seconds INTEGER NOT NULL,
    started_at_unix_seconds INTEGER,
    finished_at_unix_seconds INTEGER,
    error_payload TEXT
);

CREATE TABLE release_artwork (
    id TEXT PRIMARY KEY,
    release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    release_instance_id TEXT REFERENCES release_instances(id) ON DELETE CASCADE,
    source TEXT NOT NULL CHECK (source IN ('operator_selected', 'source_local', 'provider')),
    is_primary INTEGER NOT NULL CHECK (is_primary IN (0, 1)),
    original_path TEXT,
    managed_filename TEXT,
    mime_type TEXT NOT NULL
);

CREATE TABLE manual_overrides (
    id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('release', 'release_instance', 'track')),
    subject_id TEXT NOT NULL,
    field TEXT NOT NULL CHECK (
        field IN (
            'title',
            'album_artist',
            'artist_credit',
            'track_title',
            'release_date',
            'edition_qualifier',
            'artwork_selection'
        )
    ),
    value TEXT NOT NULL,
    note TEXT,
    created_by TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL
);

CREATE TABLE config_snapshots (
    id TEXT PRIMARY KEY,
    release_instance_id TEXT REFERENCES release_instances(id) ON DELETE SET NULL,
    fingerprint TEXT NOT NULL,
    content TEXT NOT NULL,
    captured_at_unix_seconds INTEGER NOT NULL
);
