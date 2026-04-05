DROP INDEX IF EXISTS idx_ingest_evidence_batch;
DROP INDEX IF EXISTS idx_staging_manifests_batch;
DROP INDEX IF EXISTS idx_metadata_snapshots_subject;
DROP TABLE IF EXISTS ingest_evidence_records;
DROP TABLE IF EXISTS staging_manifests;

CREATE TABLE metadata_snapshots_v1 (
    id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('release_instance', 'file')
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

INSERT INTO metadata_snapshots_v1 (
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
FROM metadata_snapshots
WHERE subject_kind IN ('release_instance', 'file');

DROP TABLE metadata_snapshots;
ALTER TABLE metadata_snapshots_v1 RENAME TO metadata_snapshots;
