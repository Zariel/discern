CREATE TABLE metadata_snapshots_v2 (
    id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('import_batch', 'release_instance', 'file')
    ),
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

INSERT INTO metadata_snapshots_v2 (
    id,
    subject_kind,
    subject_id,
    source,
    format,
    payload,
    captured_at_unix_seconds
)
SELECT
    id,
    subject_kind,
    subject_id,
    source,
    format,
    payload,
    captured_at_unix_seconds
FROM metadata_snapshots;

DROP TABLE metadata_snapshots;
ALTER TABLE metadata_snapshots_v2 RENAME TO metadata_snapshots;

CREATE TABLE staging_manifests (
    id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL CHECK (
        source_kind IN ('watch_directory', 'api_client', 'manual_add', 'gazelle')
    ),
    source_path TEXT NOT NULL,
    discovered_files_json TEXT NOT NULL,
    auxiliary_files_json TEXT NOT NULL,
    grouping_strategy TEXT NOT NULL CHECK (
        grouping_strategy IN (
            'common_parent_directory',
            'shared_album_metadata',
            'track_number_continuity',
            'manual_manifest'
        )
    ),
    grouping_groups_json TEXT NOT NULL,
    grouping_notes_json TEXT NOT NULL,
    captured_at_unix_seconds INTEGER NOT NULL
);

CREATE TABLE ingest_evidence_records (
    id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('discovered_path', 'grouped_release_input')
    ),
    subject_value TEXT NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN (
            'embedded_tags',
            'file_name',
            'directory_structure',
            'gazelle_yaml',
            'auxiliary_file'
        )
    ),
    observations_json TEXT NOT NULL,
    structured_payload TEXT,
    captured_at_unix_seconds INTEGER NOT NULL
);

CREATE INDEX idx_metadata_snapshots_subject
    ON metadata_snapshots (subject_kind, subject_id, captured_at_unix_seconds DESC);

CREATE INDEX idx_staging_manifests_batch
    ON staging_manifests (batch_id, captured_at_unix_seconds DESC);

CREATE INDEX idx_ingest_evidence_batch
    ON ingest_evidence_records (batch_id, captured_at_unix_seconds DESC);
