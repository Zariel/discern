CREATE INDEX idx_release_groups_title_artist
    ON release_groups (normalized_title, primary_artist_id);

CREATE INDEX idx_releases_release_group
    ON releases (release_group_id);

CREATE INDEX idx_release_instances_release_id
    ON release_instances (release_id);

CREATE INDEX idx_release_instances_source_path
    ON release_instances (original_source_path);

CREATE INDEX idx_issues_state_type
    ON issues (state, issue_type);

CREATE INDEX idx_jobs_status_type
    ON jobs (status, job_type);

CREATE UNIQUE INDEX idx_sources_locator_unique
    ON sources (kind, locator_kind, locator_value);

CREATE UNIQUE INDEX idx_files_managed_path_unique
    ON files (path)
    WHERE role = 'managed';

CREATE UNIQUE INDEX idx_files_source_path_unique
    ON files (path)
    WHERE role = 'source';

CREATE INDEX idx_exported_metadata_album_title
    ON exported_metadata_snapshots (album_title);
