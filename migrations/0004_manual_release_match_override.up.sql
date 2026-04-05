PRAGMA foreign_keys = OFF;

ALTER TABLE manual_overrides RENAME TO manual_overrides_old;

CREATE TABLE manual_overrides (
    id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('release', 'release_instance', 'track')),
    subject_id TEXT NOT NULL,
    field TEXT NOT NULL CHECK (
        field IN (
            'release_match',
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

INSERT INTO manual_overrides
(id, subject_kind, subject_id, field, value, note, created_by, created_at_unix_seconds)
SELECT id, subject_kind, subject_id, field, value, note, created_by,
       created_at_unix_seconds
FROM manual_overrides_old;

DROP TABLE manual_overrides_old;

PRAGMA foreign_keys = ON;
