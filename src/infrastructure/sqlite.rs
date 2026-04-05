use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;

use crate::application::repository::{
    ExportRepository, ExportedMetadataListQuery, ImportBatchCommandRepository,
    ImportBatchListQuery, ImportBatchRepository, IssueCommandRepository, IssueListQuery,
    IssueRepository, JobCommandRepository, JobListQuery, JobRepository, ReleaseGroupSearchQuery,
    ReleaseInstanceListQuery, ReleaseInstanceRepository, ReleaseListQuery, ReleaseRepository,
    RepositoryError, RepositoryErrorKind, SourceCommandRepository, SourceRepository,
};
use crate::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
    EvidenceNote, ProviderProvenance,
};
use crate::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType};
use crate::domain::release::{PartialDate, Release, ReleaseEdition};
use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use crate::domain::release_instance::{
    BitrateMode, FormatFamily, GazelleReference, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
    ReleaseInstanceState, TechnicalVariant,
};
use crate::domain::source::{Source, SourceKind, SourceLocator};
use crate::support::ids::{
    ArtistId, CandidateMatchId, DiscogsReleaseId, ExportedMetadataSnapshotId, ImportBatchId,
    IssueId, JobId, MusicBrainzReleaseGroupId, MusicBrainzReleaseId, ReleaseGroupId, ReleaseId,
    ReleaseInstanceId, SourceId, TrackInstanceId,
};
use crate::support::pagination::{Page, PageRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteInfrastructure {
    pub database_path: PathBuf,
}

impl SqliteInfrastructure {
    pub fn new(database_path: PathBuf) -> Self {
        Self { database_path }
    }
}

#[derive(Clone)]
pub struct SqliteRepositoryContext {
    database_path: PathBuf,
    writer: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for SqliteRepositoryContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteRepositoryContext")
            .field("database_path", &self.database_path)
            .finish_non_exhaustive()
    }
}

impl SqliteRepositoryContext {
    pub fn open(database_path: impl Into<PathBuf>) -> Result<Self, RepositoryError> {
        let database_path = database_path.into();
        let connection = Connection::open(&database_path)
            .map_err(|error| storage_error(format!("failed to open sqlite database: {error}")))?;
        configure_connection(&connection)?;

        Ok(Self {
            database_path,
            writer: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn read_connection(&self) -> Result<Connection, RepositoryError> {
        let connection = Connection::open(&self.database_path)
            .map_err(|error| storage_error(format!("failed to open sqlite reader: {error}")))?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    pub fn with_write_transaction<T>(
        &self,
        write: impl FnOnce(&Transaction<'_>) -> Result<T, RepositoryError>,
    ) -> Result<T, RepositoryError> {
        let mut connection = self
            .writer
            .lock()
            .map_err(|_| storage_error("sqlite writer mutex was poisoned"))?;
        let transaction = connection.transaction().map_err(|error| {
            storage_error(format!("failed to start sqlite transaction: {error}"))
        })?;
        let result = write(&transaction)?;
        transaction.commit().map_err(|error| {
            storage_error(format!("failed to commit sqlite transaction: {error}"))
        })?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct SqliteRepositories {
    context: SqliteRepositoryContext,
}

impl SqliteRepositories {
    pub fn new(context: SqliteRepositoryContext) -> Self {
        Self { context }
    }
}

impl ReleaseRepository for SqliteRepositories {
    fn get_release_group(
        &self,
        id: &ReleaseGroupId,
    ) -> Result<Option<ReleaseGroup>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, primary_artist_id, title, kind, musicbrainz_release_group_id
                 FROM release_groups
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_release_group,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, release_group_id, primary_artist_id, title,
                        musicbrainz_release_id, discogs_release_id, edition_title,
                        disambiguation, country, label, catalog_number,
                        release_year, release_month, release_day
                 FROM releases
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_release,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn find_release_by_musicbrainz_id(
        &self,
        musicbrainz_release_id: &str,
    ) -> Result<Option<Release>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, release_group_id, primary_artist_id, title,
                        musicbrainz_release_id, discogs_release_id, edition_title,
                        disambiguation, country, label, catalog_number,
                        release_year, release_month, release_day
                 FROM releases
                 WHERE musicbrainz_release_id = ?1",
                params![musicbrainz_release_id],
                map_release,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn search_release_groups(
        &self,
        query: &ReleaseGroupSearchQuery,
    ) -> Result<Page<ReleaseGroup>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM release_groups rg
                 JOIN artists a ON a.id = rg.primary_artist_id
                 WHERE (?1 IS NULL OR rg.title LIKE '%' || ?1 || '%')
                   AND (?2 IS NULL OR a.name LIKE '%' || ?2 || '%')",
                params![query.text.as_deref(), query.primary_artist_name.as_deref()],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;

        let mut statement = connection
            .prepare(
                "SELECT rg.id, rg.primary_artist_id, rg.title, rg.kind,
                        rg.musicbrainz_release_group_id
                 FROM release_groups rg
                 JOIN artists a ON a.id = rg.primary_artist_id
                 WHERE (?1 IS NULL OR rg.title LIKE '%' || ?1 || '%')
                   AND (?2 IS NULL OR a.name LIKE '%' || ?2 || '%')
                 ORDER BY rg.title ASC
                 LIMIT ?3 OFFSET ?4",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    query.text.as_deref(),
                    query.primary_artist_name.as_deref(),
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_release_group,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }

    fn list_releases(&self, query: &ReleaseListQuery) -> Result<Page<Release>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let release_group_id = query
            .release_group_id
            .as_ref()
            .map(|value| value.as_uuid().to_string());

        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM releases
                 WHERE (?1 IS NULL OR release_group_id = ?1)
                   AND (?2 IS NULL OR title LIKE '%' || ?2 || '%')",
                params![release_group_id.as_deref(), query.text.as_deref()],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;

        let mut statement = connection
            .prepare(
                "SELECT id, release_group_id, primary_artist_id, title,
                        musicbrainz_release_id, discogs_release_id, edition_title,
                        disambiguation, country, label, catalog_number,
                        release_year, release_month, release_day
                 FROM releases
                 WHERE (?1 IS NULL OR release_group_id = ?1)
                   AND (?2 IS NULL OR title LIKE '%' || ?2 || '%')
                 ORDER BY title ASC
                 LIMIT ?3 OFFSET ?4",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    release_group_id.as_deref(),
                    query.text.as_deref(),
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_release,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }
}

impl ReleaseInstanceRepository for SqliteRepositories {
    fn get_release_instance(
        &self,
        id: &ReleaseInstanceId,
    ) -> Result<Option<ReleaseInstance>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, release_id, state, format_family, bitrate_mode,
                        bitrate_kbps, sample_rate_hz, bit_depth, track_count,
                        total_duration_seconds, ingest_origin, original_source_path,
                        imported_at_unix_seconds, gazelle_tracker, gazelle_torrent_id,
                        gazelle_release_group_id
                 FROM release_instances
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_release_instance,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn list_release_instances(
        &self,
        query: &ReleaseInstanceListQuery,
    ) -> Result<Page<ReleaseInstance>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let release_id = query
            .release_id
            .as_ref()
            .map(|value| value.as_uuid().to_string());
        let state = query.state.as_ref().map(release_instance_state_to_sql);
        let format_family = query.format_family.as_ref().map(format_family_to_sql);

        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM release_instances
                 WHERE (?1 IS NULL OR release_id = ?1)
                   AND (?2 IS NULL OR state = ?2)
                   AND (?3 IS NULL OR format_family = ?3)",
                params![
                    release_id.as_deref(),
                    state.as_deref(),
                    format_family.as_deref(),
                ],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;

        let mut statement = connection
            .prepare(
                "SELECT id, release_id, state, format_family, bitrate_mode,
                        bitrate_kbps, sample_rate_hz, bit_depth, track_count,
                        total_duration_seconds, ingest_origin, original_source_path,
                        imported_at_unix_seconds, gazelle_tracker, gazelle_torrent_id,
                        gazelle_release_group_id
                 FROM release_instances
                 WHERE (?1 IS NULL OR release_id = ?1)
                   AND (?2 IS NULL OR state = ?2)
                   AND (?3 IS NULL OR format_family = ?3)
                 ORDER BY imported_at_unix_seconds DESC
                 LIMIT ?4 OFFSET ?5",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    release_id.as_deref(),
                    state.as_deref(),
                    format_family.as_deref(),
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_release_instance,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }

    fn list_candidate_matches(
        &self,
        release_instance_id: &ReleaseInstanceId,
        page: &PageRequest,
    ) -> Result<Page<CandidateMatch>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM candidate_matches
                 WHERE release_instance_id = ?1",
                params![release_instance_id.as_uuid().to_string()],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, release_instance_id, provider, candidate_kind,
                        provider_entity_id, normalized_score, evidence_matches_json,
                        mismatches_json, unresolved_ambiguities_json,
                        provider_provenance_json
                 FROM candidate_matches
                 WHERE release_instance_id = ?1
                 ORDER BY normalized_score DESC
                 LIMIT ?2 OFFSET ?3",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    release_instance_id.as_uuid().to_string(),
                    i64::from(page.limit),
                    page.offset as i64,
                ],
                map_candidate_match,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: *page,
            total: total as u64,
        })
    }

    fn get_candidate_match(
        &self,
        id: &CandidateMatchId,
    ) -> Result<Option<CandidateMatch>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, release_instance_id, provider, candidate_kind,
                        provider_entity_id, normalized_score, evidence_matches_json,
                        mismatches_json, unresolved_ambiguities_json,
                        provider_provenance_json
                 FROM candidate_matches
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_candidate_match,
            )
            .optional()
            .map_err(to_storage_error)
    }
}

impl ImportBatchRepository for SqliteRepositories {
    fn get_import_batch(&self, id: &ImportBatchId) -> Result<Option<ImportBatch>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut batch = connection
            .query_row(
                "SELECT id, source_id, mode, status, requested_by_kind,
                        requested_by_name, created_at_unix_seconds
                 FROM import_batches
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_import_batch,
            )
            .optional()
            .map_err(to_storage_error)?;

        if let Some(batch) = batch.as_mut() {
            batch.received_paths = load_import_batch_paths(&connection, &batch.id)?;
        }

        Ok(batch)
    }

    fn list_import_batches(
        &self,
        query: &ImportBatchListQuery,
    ) -> Result<Page<ImportBatch>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let total: i64 = connection
            .query_row("SELECT COUNT(*) FROM import_batches", [], |row| row.get(0))
            .map_err(to_storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, source_id, mode, status, requested_by_kind,
                        requested_by_name, created_at_unix_seconds
                 FROM import_batches
                 ORDER BY created_at_unix_seconds DESC
                 LIMIT ?1 OFFSET ?2",
            )
            .map_err(to_storage_error)?;
        let mut items = statement
            .query_map(
                params![i64::from(query.page.limit), query.page.offset as i64],
                map_import_batch,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;
        for batch in &mut items {
            batch.received_paths = load_import_batch_paths(&connection, &batch.id)?;
        }

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }
}

impl SourceRepository for SqliteRepositories {
    fn find_source_by_locator(
        &self,
        locator: &SourceLocator,
    ) -> Result<Option<Source>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let (locator_kind, locator_value) = source_locator_to_sql(locator);
        connection
            .query_row(
                "SELECT id, kind, display_name, locator_kind, locator_value, external_reference
                 FROM sources
                 WHERE locator_kind = ?1 AND locator_value = ?2",
                params![locator_kind, locator_value],
                map_source,
            )
            .optional()
            .map_err(to_storage_error)
    }
}

impl SourceCommandRepository for SqliteRepositories {
    fn create_source(&self, source: &Source) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            let (locator_kind, locator_value) = source_locator_to_sql(&source.locator);
            transaction
                .execute(
                    "INSERT INTO sources
                     (id, kind, display_name, locator_kind, locator_value, external_reference)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        source.id.as_uuid().to_string(),
                        source_kind_to_sql(&source.kind),
                        &source.display_name,
                        locator_kind,
                        locator_value,
                        &source.external_reference,
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }
}

impl ImportBatchCommandRepository for SqliteRepositories {
    fn create_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO import_batches
                     (id, source_id, mode, status, requested_by_kind, requested_by_name,
                      created_at_unix_seconds)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        batch.id.as_uuid().to_string(),
                        batch.source_id.as_uuid().to_string(),
                        import_mode_to_sql(&batch.mode),
                        import_batch_status_to_sql(&batch.status),
                        batch_requester_kind_to_sql(&batch.requested_by),
                        batch_requester_name_to_sql(&batch.requested_by),
                        batch.created_at_unix_seconds,
                    ],
                )
                .map_err(to_storage_error)?;

            for (ordinal, path) in batch.received_paths.iter().enumerate() {
                transaction
                    .execute(
                        "INSERT INTO import_batch_paths (import_batch_id, ordinal, path)
                         VALUES (?1, ?2, ?3)",
                        params![
                            batch.id.as_uuid().to_string(),
                            ordinal as i64,
                            path.to_string_lossy().to_string(),
                        ],
                    )
                    .map_err(to_storage_error)?;
            }

            Ok(())
        })
    }

    fn list_active_import_batches_for_source(
        &self,
        source_id: &SourceId,
    ) -> Result<Vec<ImportBatch>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, source_id, mode, status, requested_by_kind,
                        requested_by_name, created_at_unix_seconds
                 FROM import_batches
                 WHERE source_id = ?1
                   AND status IN ('created', 'discovering', 'grouped', 'submitted')
                 ORDER BY created_at_unix_seconds DESC",
            )
            .map_err(to_storage_error)?;
        let mut items = statement
            .query_map(params![source_id.as_uuid().to_string()], map_import_batch)
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;
        for batch in &mut items {
            batch.received_paths = load_import_batch_paths(&connection, &batch.id)?;
        }
        Ok(items)
    }
}

impl IssueRepository for SqliteRepositories {
    fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, issue_type, state, subject_kind, subject_id, summary,
                        details, created_at_unix_seconds, resolved_at_unix_seconds,
                        suppressed_reason
                 FROM issues
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_issue,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let state = query.state.as_ref().map(issue_state_to_sql);
        let issue_type = query.issue_type.as_ref().map(issue_type_to_sql);
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM issues
                 WHERE (?1 IS NULL OR state = ?1)
                   AND (?2 IS NULL OR issue_type = ?2)",
                params![state.as_deref(), issue_type.as_deref()],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, issue_type, state, subject_kind, subject_id, summary,
                        details, created_at_unix_seconds, resolved_at_unix_seconds,
                        suppressed_reason
                 FROM issues
                 WHERE (?1 IS NULL OR state = ?1)
                   AND (?2 IS NULL OR issue_type = ?2)
                 ORDER BY created_at_unix_seconds DESC
                 LIMIT ?3 OFFSET ?4",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    state.as_deref(),
                    issue_type.as_deref(),
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_issue,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }
}

impl IssueCommandRepository for SqliteRepositories {
    fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO issues
                     (id, issue_type, state, subject_kind, subject_id, summary,
                      details, created_at_unix_seconds, resolved_at_unix_seconds,
                      suppressed_reason)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        issue.id.as_uuid().to_string(),
                        issue_type_to_sql(&issue.issue_type),
                        issue_state_to_sql(&issue.state),
                        issue_subject_kind_to_sql(&issue.subject),
                        issue_subject_id_to_sql(&issue.subject),
                        &issue.summary,
                        &issue.details,
                        issue.created_at_unix_seconds,
                        issue.resolved_at_unix_seconds,
                        &issue.suppressed_reason,
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }

    fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            let changed = transaction
                .execute(
                    "UPDATE issues
                     SET issue_type = ?2,
                         state = ?3,
                         subject_kind = ?4,
                         subject_id = ?5,
                         summary = ?6,
                         details = ?7,
                         created_at_unix_seconds = ?8,
                         resolved_at_unix_seconds = ?9,
                         suppressed_reason = ?10
                     WHERE id = ?1",
                    params![
                        issue.id.as_uuid().to_string(),
                        issue_type_to_sql(&issue.issue_type),
                        issue_state_to_sql(&issue.state),
                        issue_subject_kind_to_sql(&issue.subject),
                        issue_subject_id_to_sql(&issue.subject),
                        &issue.summary,
                        &issue.details,
                        issue.created_at_unix_seconds,
                        issue.resolved_at_unix_seconds,
                        &issue.suppressed_reason,
                    ],
                )
                .map_err(to_storage_error)?;

            if changed == 0 {
                return Err(RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: format!("issue {} was not found", issue.id.as_uuid()),
                });
            }

            Ok(())
        })
    }
}

impl JobRepository for SqliteRepositories {
    fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, job_type, subject_kind, subject_id, status,
                        progress_phase, retry_count, triggered_by,
                        created_at_unix_seconds, started_at_unix_seconds,
                        finished_at_unix_seconds, error_payload
                 FROM jobs
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_job,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn list_jobs(&self, query: &JobListQuery) -> Result<Page<Job>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let status = query.status.as_ref().map(job_status_to_sql);
        let job_type = query.job_type.as_ref().map(job_type_to_sql);
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM jobs
                 WHERE (?1 IS NULL OR status = ?1)
                   AND (?2 IS NULL OR job_type = ?2)",
                params![status.as_deref(), job_type.as_deref()],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, job_type, subject_kind, subject_id, status,
                        progress_phase, retry_count, triggered_by,
                        created_at_unix_seconds, started_at_unix_seconds,
                        finished_at_unix_seconds, error_payload
                 FROM jobs
                 WHERE (?1 IS NULL OR status = ?1)
                   AND (?2 IS NULL OR job_type = ?2)
                 ORDER BY created_at_unix_seconds DESC
                 LIMIT ?3 OFFSET ?4",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    status.as_deref(),
                    job_type.as_deref(),
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_job,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }
}

impl JobCommandRepository for SqliteRepositories {
    fn create_job(&self, job: &Job) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO jobs
                     (id, job_type, subject_kind, subject_id, status, progress_phase,
                      retry_count, triggered_by, created_at_unix_seconds,
                      started_at_unix_seconds, finished_at_unix_seconds, error_payload)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        job.id.as_uuid().to_string(),
                        job_type_to_sql(&job.job_type),
                        job_subject_kind_to_sql(&job.subject),
                        job_subject_id_to_sql(&job.subject),
                        job_status_to_sql(&job.status),
                        &job.progress_phase,
                        i64::from(job.retry_count),
                        job_trigger_to_sql(&job.triggered_by),
                        job.created_at_unix_seconds,
                        job.started_at_unix_seconds,
                        job.finished_at_unix_seconds,
                        &job.error_payload,
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }

    fn update_job(&self, job: &Job) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET job_type = ?2,
                         subject_kind = ?3,
                         subject_id = ?4,
                         status = ?5,
                         progress_phase = ?6,
                         retry_count = ?7,
                         triggered_by = ?8,
                         created_at_unix_seconds = ?9,
                         started_at_unix_seconds = ?10,
                         finished_at_unix_seconds = ?11,
                         error_payload = ?12
                     WHERE id = ?1",
                    params![
                        job.id.as_uuid().to_string(),
                        job_type_to_sql(&job.job_type),
                        job_subject_kind_to_sql(&job.subject),
                        job_subject_id_to_sql(&job.subject),
                        job_status_to_sql(&job.status),
                        &job.progress_phase,
                        i64::from(job.retry_count),
                        job_trigger_to_sql(&job.triggered_by),
                        job.created_at_unix_seconds,
                        job.started_at_unix_seconds,
                        job.finished_at_unix_seconds,
                        &job.error_payload,
                    ],
                )
                .map_err(to_storage_error)?;
            if changed == 0 {
                return Err(RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: format!("job {} was not found", job.id.as_uuid()),
                });
            }
            Ok(())
        })
    }

    fn list_recoverable_jobs(&self) -> Result<Vec<Job>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, job_type, subject_kind, subject_id, status,
                        progress_phase, retry_count, triggered_by,
                        created_at_unix_seconds, started_at_unix_seconds,
                        finished_at_unix_seconds, error_payload
                 FROM jobs
                 WHERE status IN ('queued', 'running')
                 ORDER BY created_at_unix_seconds ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map([], map_job)
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }
}

impl ExportRepository for SqliteRepositories {
    fn get_latest_exported_metadata(
        &self,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, release_instance_id, export_profile, album_title,
                        album_artist, artist_credits_json, edition_visibility,
                        technical_visibility, path_components_json,
                        primary_artwork_filename, compatibility_verified,
                        compatibility_warnings_json, rendered_at_unix_seconds
                 FROM exported_metadata_snapshots
                 WHERE release_instance_id = ?1
                 ORDER BY rendered_at_unix_seconds DESC
                 LIMIT 1",
                params![release_instance_id.as_uuid().to_string()],
                map_exported_metadata,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn list_exported_metadata(
        &self,
        query: &ExportedMetadataListQuery,
    ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let release_instance_id = query
            .release_instance_id
            .as_ref()
            .map(|value| value.as_uuid().to_string());
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM exported_metadata_snapshots
                 WHERE (?1 IS NULL OR release_instance_id = ?1)
                   AND (?2 IS NULL OR album_title LIKE '%' || ?2 || '%')",
                params![release_instance_id.as_deref(), query.album_title.as_deref()],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, release_instance_id, export_profile, album_title,
                        album_artist, artist_credits_json, edition_visibility,
                        technical_visibility, path_components_json,
                        primary_artwork_filename, compatibility_verified,
                        compatibility_warnings_json, rendered_at_unix_seconds
                 FROM exported_metadata_snapshots
                 WHERE (?1 IS NULL OR release_instance_id = ?1)
                   AND (?2 IS NULL OR album_title LIKE '%' || ?2 || '%')
                 ORDER BY rendered_at_unix_seconds DESC
                 LIMIT ?3 OFFSET ?4",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    release_instance_id.as_deref(),
                    query.album_title.as_deref(),
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_exported_metadata,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)?;

        Ok(Page {
            items,
            request: query.page,
            total: total as u64,
        })
    }

    fn get_exported_metadata(
        &self,
        id: &ExportedMetadataSnapshotId,
    ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, release_instance_id, export_profile, album_title,
                        album_artist, artist_credits_json, edition_visibility,
                        technical_visibility, path_components_json,
                        primary_artwork_filename, compatibility_verified,
                        compatibility_warnings_json, rendered_at_unix_seconds
                 FROM exported_metadata_snapshots
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_exported_metadata,
            )
            .optional()
            .map_err(to_storage_error)
    }
}

fn configure_connection(connection: &Connection) -> Result<(), RepositoryError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(to_storage_error)?;
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )
        .map_err(to_storage_error)
}

fn map_release_group(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReleaseGroup> {
    Ok(ReleaseGroup {
        id: parse_uuid_id::<ReleaseGroupId>(row.get_ref(0)?, 0)?,
        primary_artist_id: parse_uuid_id::<ArtistId>(row.get_ref(1)?, 1)?,
        title: row.get(2)?,
        kind: parse_release_group_kind(row.get::<_, String>(3)?),
        musicbrainz_release_group_id: parse_optional_mb_release_group(row.get(4)?),
    })
}

fn map_release(row: &rusqlite::Row<'_>) -> rusqlite::Result<Release> {
    let year = row.get::<_, Option<i64>>(11)?;
    let month = row.get::<_, Option<i64>>(12)?;
    let day = row.get::<_, Option<i64>>(13)?;

    Ok(Release {
        id: parse_uuid_id::<ReleaseId>(row.get_ref(0)?, 0)?,
        release_group_id: parse_uuid_id::<ReleaseGroupId>(row.get_ref(1)?, 1)?,
        primary_artist_id: parse_uuid_id::<ArtistId>(row.get_ref(2)?, 2)?,
        title: row.get(3)?,
        musicbrainz_release_id: parse_optional_mb_release(row.get(4)?),
        discogs_release_id: row.get::<_, Option<u64>>(5)?.map(DiscogsReleaseId::new),
        edition: ReleaseEdition {
            edition_title: row.get(6)?,
            disambiguation: row.get(7)?,
            country: row.get(8)?,
            label: row.get(9)?,
            catalog_number: row.get(10)?,
            release_date: year.map(|year| PartialDate {
                year: year as u16,
                month: month.map(|value| value as u8),
                day: day.map(|value| value as u8),
            }),
        },
    })
}

fn map_release_instance(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReleaseInstance> {
    Ok(ReleaseInstance {
        id: parse_uuid_id::<ReleaseInstanceId>(row.get_ref(0)?, 0)?,
        release_id: parse_uuid_id::<ReleaseId>(row.get_ref(1)?, 1)?,
        state: parse_release_instance_state(row.get::<_, String>(2)?),
        technical_variant: TechnicalVariant {
            format_family: parse_format_family(row.get::<_, String>(3)?),
            bitrate_mode: parse_bitrate_mode(row.get::<_, String>(4)?),
            bitrate_kbps: row.get::<_, Option<i64>>(5)?.map(|value| value as u32),
            sample_rate_hz: row.get::<_, Option<i64>>(6)?.map(|value| value as u32),
            bit_depth: row.get::<_, Option<i64>>(7)?.map(|value| value as u8),
            track_count: row.get::<_, i64>(8)? as u16,
            total_duration_seconds: row.get::<_, i64>(9)? as u32,
        },
        provenance: ProvenanceSnapshot {
            ingest_origin: parse_ingest_origin(row.get::<_, String>(10)?),
            original_source_path: row.get(11)?,
            imported_at_unix_seconds: row.get(12)?,
            gazelle_reference: row
                .get::<_, Option<String>>(13)?
                .map(|tracker| GazelleReference {
                    tracker,
                    torrent_id: row.get(14).unwrap_or(None),
                    release_group_id: row.get(15).unwrap_or(None),
                }),
        },
    })
}

fn map_candidate_match(row: &rusqlite::Row<'_>) -> rusqlite::Result<CandidateMatch> {
    let provider = parse_candidate_provider(row.get::<_, String>(2)?);
    let kind = row.get::<_, String>(3)?;
    let provider_entity_id: String = row.get(4)?;
    let score = row.get::<_, f64>(5)? as f32;

    Ok(CandidateMatch {
        id: parse_uuid_id::<CandidateMatchId>(row.get_ref(0)?, 0)?,
        release_instance_id: parse_uuid_id::<ReleaseInstanceId>(row.get_ref(1)?, 1)?,
        provider: provider.clone(),
        subject: parse_candidate_subject(kind, provider_entity_id),
        normalized_score: CandidateScore::new(score),
        evidence_matches: parse_evidence_notes(row.get(6)?)
            .map_err(|error| invalid_column(6, error))?,
        mismatches: parse_evidence_notes(row.get(7)?).map_err(|error| invalid_column(7, error))?,
        unresolved_ambiguities: parse_string_list(row.get(8)?)
            .map_err(|error| invalid_column(8, error))?,
        provider_provenance: parse_provider_provenance(row.get(9)?)
            .map_err(|error| invalid_column(9, error))?,
    })
}

fn map_import_batch(row: &rusqlite::Row<'_>) -> rusqlite::Result<ImportBatch> {
    Ok(ImportBatch {
        id: parse_uuid_id::<ImportBatchId>(row.get_ref(0)?, 0)?,
        source_id: parse_uuid_id::<SourceId>(row.get_ref(1)?, 1)?,
        mode: parse_import_mode(row.get::<_, String>(2)?),
        status: parse_import_batch_status(row.get::<_, String>(3)?),
        requested_by: parse_batch_requester(row.get::<_, String>(4)?, row.get(5)?),
        created_at_unix_seconds: row.get(6)?,
        received_paths: Vec::new(),
    })
}

fn map_source(row: &rusqlite::Row<'_>) -> rusqlite::Result<Source> {
    let locator_kind = row.get::<_, String>(3)?;
    let locator_value = row.get::<_, String>(4)?;
    Ok(Source {
        id: parse_uuid_id::<SourceId>(row.get_ref(0)?, 0)?,
        kind: parse_source_kind(row.get(1)?),
        display_name: row.get(2)?,
        locator: parse_source_locator(locator_kind, locator_value)
            .map_err(|error| invalid_column(4, error))?,
        external_reference: row.get(5)?,
    })
}

fn map_issue(row: &rusqlite::Row<'_>) -> rusqlite::Result<Issue> {
    let subject_kind: String = row.get(3)?;
    let subject_id: Option<String> = row.get(4)?;
    Ok(Issue {
        id: parse_uuid_id::<IssueId>(row.get_ref(0)?, 0)?,
        issue_type: parse_issue_type(row.get::<_, String>(1)?),
        state: parse_issue_state(row.get::<_, String>(2)?),
        subject: parse_issue_subject(subject_kind, subject_id)
            .map_err(|error| invalid_column(4, error))?,
        summary: row.get(5)?,
        details: row.get(6)?,
        created_at_unix_seconds: row.get(7)?,
        resolved_at_unix_seconds: row.get(8)?,
        suppressed_reason: row.get(9)?,
    })
}

fn map_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let subject_kind: String = row.get(2)?;
    let subject_id: String = row.get(3)?;
    Ok(Job {
        id: parse_uuid_id::<JobId>(row.get_ref(0)?, 0)?,
        job_type: parse_job_type(row.get::<_, String>(1)?),
        subject: parse_job_subject(subject_kind, subject_id)
            .map_err(|error| invalid_column(3, error))?,
        status: parse_job_status(row.get::<_, String>(4)?),
        progress_phase: row.get(5)?,
        retry_count: row.get::<_, i64>(6)? as u16,
        triggered_by: parse_job_trigger(row.get::<_, String>(7)?),
        created_at_unix_seconds: row.get(8)?,
        started_at_unix_seconds: row.get(9)?,
        finished_at_unix_seconds: row.get(10)?,
        error_payload: row.get(11)?,
    })
}

fn map_exported_metadata(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExportedMetadataSnapshot> {
    Ok(ExportedMetadataSnapshot {
        id: parse_uuid_id::<ExportedMetadataSnapshotId>(row.get_ref(0)?, 0)?,
        release_instance_id: parse_uuid_id::<ReleaseInstanceId>(row.get_ref(1)?, 1)?,
        export_profile: row.get(2)?,
        album_title: row.get(3)?,
        album_artist: row.get(4)?,
        artist_credits: parse_string_list(row.get(5)?).map_err(|error| invalid_column(5, error))?,
        edition_visibility: parse_qualifier_visibility(row.get::<_, String>(6)?),
        technical_visibility: parse_qualifier_visibility(row.get::<_, String>(7)?),
        path_components: parse_string_list(row.get(8)?)
            .map_err(|error| invalid_column(8, error))?,
        primary_artwork_filename: row.get(9)?,
        compatibility: CompatibilityReport {
            verified: row.get::<_, i64>(10)? == 1,
            warnings: parse_string_list(row.get(11)?).map_err(|error| invalid_column(11, error))?,
        },
        rendered_at_unix_seconds: row.get(12)?,
    })
}

fn load_import_batch_paths(
    connection: &Connection,
    import_batch_id: &ImportBatchId,
) -> Result<Vec<PathBuf>, RepositoryError> {
    let mut statement = connection
        .prepare(
            "SELECT path
             FROM import_batch_paths
             WHERE import_batch_id = ?1
             ORDER BY ordinal ASC",
        )
        .map_err(to_storage_error)?;
    let paths = statement
        .query_map(params![import_batch_id.as_uuid().to_string()], |row| {
            row.get::<_, String>(0).map(PathBuf::from)
        })
        .map_err(to_storage_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(to_storage_error)?;
    Ok(paths)
}

fn parse_uuid_id<T>(value: rusqlite::types::ValueRef<'_>, column: usize) -> rusqlite::Result<T>
where
    T: ParseUuidId,
{
    let raw = value.as_str()?;
    T::parse(raw).map_err(|error| invalid_column(column, error))
}

trait ParseUuidId: Sized {
    fn parse(value: &str) -> Result<Self, String>;
}

macro_rules! impl_parse_uuid_id {
    ($($name:ty),+ $(,)?) => {
        $(
            impl ParseUuidId for $name {
                fn parse(value: &str) -> Result<Self, String> {
                    <$name>::parse_str(value).map_err(|error| error.to_string())
                }
            }
        )+
    };
}

impl_parse_uuid_id!(
    ArtistId,
    CandidateMatchId,
    ExportedMetadataSnapshotId,
    ImportBatchId,
    IssueId,
    JobId,
    ReleaseGroupId,
    ReleaseId,
    ReleaseInstanceId,
    SourceId,
    TrackInstanceId,
    MusicBrainzReleaseGroupId,
    MusicBrainzReleaseId
);

fn parse_optional_mb_release_group(raw: Option<String>) -> Option<MusicBrainzReleaseGroupId> {
    raw.and_then(|value| MusicBrainzReleaseGroupId::parse_str(&value).ok())
}

fn parse_optional_mb_release(raw: Option<String>) -> Option<MusicBrainzReleaseId> {
    raw.and_then(|value| MusicBrainzReleaseId::parse_str(&value).ok())
}

fn parse_release_group_kind(value: String) -> ReleaseGroupKind {
    match value.as_str() {
        "album" => ReleaseGroupKind::Album,
        "ep" => ReleaseGroupKind::Ep,
        "single" => ReleaseGroupKind::Single,
        "live" => ReleaseGroupKind::Live,
        "compilation" => ReleaseGroupKind::Compilation,
        "soundtrack" => ReleaseGroupKind::Soundtrack,
        other => ReleaseGroupKind::Other(other.to_string()),
    }
}

fn parse_release_instance_state(value: String) -> ReleaseInstanceState {
    match value.as_str() {
        "discovered" => ReleaseInstanceState::Discovered,
        "staged" => ReleaseInstanceState::Staged,
        "analyzed" => ReleaseInstanceState::Analyzed,
        "matched" => ReleaseInstanceState::Matched,
        "needs_review" => ReleaseInstanceState::NeedsReview,
        "rendering_export" => ReleaseInstanceState::RenderingExport,
        "tagging" => ReleaseInstanceState::Tagging,
        "organizing" => ReleaseInstanceState::Organizing,
        "imported" => ReleaseInstanceState::Imported,
        "verified" => ReleaseInstanceState::Verified,
        "quarantined" => ReleaseInstanceState::Quarantined,
        _ => ReleaseInstanceState::Failed,
    }
}

fn release_instance_state_to_sql(value: &ReleaseInstanceState) -> String {
    match value {
        ReleaseInstanceState::Discovered => "discovered",
        ReleaseInstanceState::Staged => "staged",
        ReleaseInstanceState::Analyzed => "analyzed",
        ReleaseInstanceState::Matched => "matched",
        ReleaseInstanceState::NeedsReview => "needs_review",
        ReleaseInstanceState::RenderingExport => "rendering_export",
        ReleaseInstanceState::Tagging => "tagging",
        ReleaseInstanceState::Organizing => "organizing",
        ReleaseInstanceState::Imported => "imported",
        ReleaseInstanceState::Verified => "verified",
        ReleaseInstanceState::Quarantined => "quarantined",
        ReleaseInstanceState::Failed => "failed",
    }
    .to_string()
}

fn parse_format_family(value: String) -> FormatFamily {
    match value.as_str() {
        "flac" => FormatFamily::Flac,
        _ => FormatFamily::Mp3,
    }
}

fn format_family_to_sql(value: &FormatFamily) -> String {
    match value {
        FormatFamily::Flac => "flac",
        FormatFamily::Mp3 => "mp3",
    }
    .to_string()
}

fn parse_bitrate_mode(value: String) -> BitrateMode {
    match value.as_str() {
        "constant" => BitrateMode::Constant,
        "variable" => BitrateMode::Variable,
        _ => BitrateMode::Lossless,
    }
}

fn parse_ingest_origin(value: String) -> IngestOrigin {
    match value.as_str() {
        "watch_directory" => IngestOrigin::WatchDirectory,
        "api_push" => IngestOrigin::ApiPush,
        _ => IngestOrigin::ManualAdd,
    }
}

fn parse_candidate_provider(value: String) -> CandidateProvider {
    match value.as_str() {
        "musicbrainz" => CandidateProvider::MusicBrainz,
        _ => CandidateProvider::Discogs,
    }
}

fn parse_candidate_subject(kind: String, provider_id: String) -> CandidateSubject {
    match kind.as_str() {
        "release" => CandidateSubject::Release { provider_id },
        _ => CandidateSubject::ReleaseGroup { provider_id },
    }
}

fn parse_evidence_notes(raw: String) -> Result<Vec<EvidenceNote>, String> {
    let value: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    let items = value
        .as_array()
        .ok_or_else(|| "evidence notes must be a JSON array".to_string())?;

    items
        .iter()
        .map(|item| {
            let object = item
                .as_object()
                .ok_or_else(|| "evidence note must be a JSON object".to_string())?;
            let kind = object
                .get("kind")
                .and_then(Value::as_str)
                .ok_or_else(|| "evidence note kind must be a string".to_string())?;
            let detail = object
                .get("detail")
                .and_then(Value::as_str)
                .ok_or_else(|| "evidence note detail must be a string".to_string())?;
            Ok(EvidenceNote {
                kind: parse_evidence_kind(kind),
                detail: detail.to_string(),
            })
        })
        .collect()
}

fn parse_evidence_kind(value: &str) -> EvidenceKind {
    match value {
        "artist_match" => EvidenceKind::ArtistMatch,
        "album_title_match" => EvidenceKind::AlbumTitleMatch,
        "track_count_match" => EvidenceKind::TrackCountMatch,
        "duration_alignment" => EvidenceKind::DurationAlignment,
        "disc_count_match" => EvidenceKind::DiscCountMatch,
        "date_proximity" => EvidenceKind::DateProximity,
        "label_catalog_alignment" => EvidenceKind::LabelCatalogAlignment,
        "filename_similarity" => EvidenceKind::FilenameSimilarity,
        "gazelle_consistency" => EvidenceKind::GazelleConsistency,
        other => EvidenceKind::Other(other.to_string()),
    }
}

fn parse_provider_provenance(raw: String) -> Result<ProviderProvenance, String> {
    let value: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    let object = value
        .as_object()
        .ok_or_else(|| "provider provenance must be a JSON object".to_string())?;
    Ok(ProviderProvenance {
        provider_name: get_json_string(object, "provider_name")?,
        query: get_json_string(object, "query")?,
        fetched_at_unix_seconds: get_json_i64(object, "fetched_at_unix_seconds")?,
    })
}

fn parse_string_list(raw: String) -> Result<Vec<String>, String> {
    let value: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    value
        .as_array()
        .ok_or_else(|| "expected a JSON array".to_string())?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| "expected array entries to be strings".to_string())
        })
        .collect()
}

fn get_json_string(object: &serde_json::Map<String, Value>, key: &str) -> Result<String, String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing string field '{key}'"))
}

fn get_json_i64(object: &serde_json::Map<String, Value>, key: &str) -> Result<i64, String> {
    object
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("missing integer field '{key}'"))
}

fn parse_import_mode(value: String) -> ImportMode {
    match value.as_str() {
        "copy" => ImportMode::Copy,
        "move" => ImportMode::Move,
        _ => ImportMode::Hardlink,
    }
}

fn import_mode_to_sql(value: &ImportMode) -> &'static str {
    match value {
        ImportMode::Copy => "copy",
        ImportMode::Move => "move",
        ImportMode::Hardlink => "hardlink",
    }
}

fn parse_import_batch_status(value: String) -> ImportBatchStatus {
    match value.as_str() {
        "created" => ImportBatchStatus::Created,
        "discovering" => ImportBatchStatus::Discovering,
        "grouped" => ImportBatchStatus::Grouped,
        "submitted" => ImportBatchStatus::Submitted,
        "quarantined" => ImportBatchStatus::Quarantined,
        _ => ImportBatchStatus::Failed,
    }
}

fn import_batch_status_to_sql(value: &ImportBatchStatus) -> &'static str {
    match value {
        ImportBatchStatus::Created => "created",
        ImportBatchStatus::Discovering => "discovering",
        ImportBatchStatus::Grouped => "grouped",
        ImportBatchStatus::Submitted => "submitted",
        ImportBatchStatus::Quarantined => "quarantined",
        ImportBatchStatus::Failed => "failed",
    }
}

fn parse_batch_requester(kind: String, name: Option<String>) -> BatchRequester {
    match kind.as_str() {
        "system" => BatchRequester::System,
        "operator" => BatchRequester::Operator {
            name: name.unwrap_or_else(|| "operator".to_string()),
        },
        _ => BatchRequester::ExternalClient {
            name: name.unwrap_or_else(|| "external".to_string()),
        },
    }
}

fn batch_requester_kind_to_sql(value: &BatchRequester) -> &'static str {
    match value {
        BatchRequester::System => "system",
        BatchRequester::Operator { .. } => "operator",
        BatchRequester::ExternalClient { .. } => "external_client",
    }
}

fn batch_requester_name_to_sql(value: &BatchRequester) -> Option<String> {
    match value {
        BatchRequester::System => None,
        BatchRequester::Operator { name } => Some(name.clone()),
        BatchRequester::ExternalClient { name } => Some(name.clone()),
    }
}

fn parse_source_kind(value: String) -> SourceKind {
    match value.as_str() {
        "watch_directory" => SourceKind::WatchDirectory,
        "api_client" => SourceKind::ApiClient,
        "manual_add" => SourceKind::ManualAdd,
        _ => SourceKind::Gazelle,
    }
}

fn source_kind_to_sql(value: &SourceKind) -> &'static str {
    match value {
        SourceKind::WatchDirectory => "watch_directory",
        SourceKind::ApiClient => "api_client",
        SourceKind::ManualAdd => "manual_add",
        SourceKind::Gazelle => "gazelle",
    }
}

fn parse_source_locator(kind: String, value: String) -> Result<SourceLocator, String> {
    match kind.as_str() {
        "filesystem_path" => Ok(SourceLocator::FilesystemPath(value.into())),
        "api_client" => Ok(SourceLocator::ApiClient { client_name: value }),
        "manual_entry" => Ok(SourceLocator::ManualEntry {
            submitted_path: value.into(),
        }),
        "tracker_ref" => {
            let mut parts = value.splitn(2, ':');
            let tracker = parts.next().unwrap_or_default().to_string();
            let identifier = parts
                .next()
                .ok_or_else(|| format!("invalid tracker_ref locator '{value}'"))?
                .to_string();
            Ok(SourceLocator::TrackerRef {
                tracker,
                identifier,
            })
        }
        other => Err(format!("unknown source locator kind '{other}'")),
    }
}

fn source_locator_to_sql(locator: &SourceLocator) -> (&'static str, String) {
    match locator {
        SourceLocator::FilesystemPath(path) => {
            ("filesystem_path", path.to_string_lossy().to_string())
        }
        SourceLocator::ApiClient { client_name } => ("api_client", client_name.clone()),
        SourceLocator::ManualEntry { submitted_path } => {
            ("manual_entry", submitted_path.to_string_lossy().to_string())
        }
        SourceLocator::TrackerRef {
            tracker,
            identifier,
        } => ("tracker_ref", format!("{tracker}:{identifier}")),
    }
}

fn parse_issue_type(value: String) -> IssueType {
    match value.as_str() {
        "unmatched_release" => IssueType::UnmatchedRelease,
        "ambiguous_release_match" => IssueType::AmbiguousReleaseMatch,
        "conflicting_metadata" => IssueType::ConflictingMetadata,
        "inconsistent_track_count" => IssueType::InconsistentTrackCount,
        "missing_tracks" => IssueType::MissingTracks,
        "corrupt_file" => IssueType::CorruptFile,
        "unsupported_format" => IssueType::UnsupportedFormat,
        "duplicate_release_instance" => IssueType::DuplicateReleaseInstance,
        "undistinguishable_release_instance" => IssueType::UndistinguishableReleaseInstance,
        "player_visibility_collision" => IssueType::PlayerVisibilityCollision,
        "missing_artwork" => IssueType::MissingArtwork,
        "broken_tags" => IssueType::BrokenTags,
        "multi_disc_ambiguity" => IssueType::MultiDiscAmbiguity,
        "compilation_artist_ambiguity" => IssueType::CompilationArtistAmbiguity,
        _ => IssueType::PlayerCompatibilityFailure,
    }
}

fn issue_type_to_sql(value: &IssueType) -> String {
    match value {
        IssueType::UnmatchedRelease => "unmatched_release",
        IssueType::AmbiguousReleaseMatch => "ambiguous_release_match",
        IssueType::ConflictingMetadata => "conflicting_metadata",
        IssueType::InconsistentTrackCount => "inconsistent_track_count",
        IssueType::MissingTracks => "missing_tracks",
        IssueType::CorruptFile => "corrupt_file",
        IssueType::UnsupportedFormat => "unsupported_format",
        IssueType::DuplicateReleaseInstance => "duplicate_release_instance",
        IssueType::UndistinguishableReleaseInstance => "undistinguishable_release_instance",
        IssueType::PlayerVisibilityCollision => "player_visibility_collision",
        IssueType::MissingArtwork => "missing_artwork",
        IssueType::BrokenTags => "broken_tags",
        IssueType::MultiDiscAmbiguity => "multi_disc_ambiguity",
        IssueType::CompilationArtistAmbiguity => "compilation_artist_ambiguity",
        IssueType::PlayerCompatibilityFailure => "player_compatibility_failure",
    }
    .to_string()
}

fn parse_issue_state(value: String) -> IssueState {
    match value.as_str() {
        "open" => IssueState::Open,
        "resolved" => IssueState::Resolved,
        _ => IssueState::Suppressed,
    }
}

fn issue_state_to_sql(value: &IssueState) -> String {
    match value {
        IssueState::Open => "open",
        IssueState::Resolved => "resolved",
        IssueState::Suppressed => "suppressed",
    }
    .to_string()
}

fn parse_issue_subject(kind: String, id: Option<String>) -> Result<IssueSubject, String> {
    match kind.as_str() {
        "release" => Ok(IssueSubject::Release(parse_release_id(id)?)),
        "release_instance" => Ok(IssueSubject::ReleaseInstance(parse_release_instance_id(
            id,
        )?)),
        "track_instance" => Ok(IssueSubject::TrackInstance(parse_track_instance_id(id)?)),
        "library" => Ok(IssueSubject::Library),
        other => Err(format!("unknown issue subject kind '{other}'")),
    }
}

fn issue_subject_kind_to_sql(subject: &IssueSubject) -> &'static str {
    match subject {
        IssueSubject::Release(_) => "release",
        IssueSubject::ReleaseInstance(_) => "release_instance",
        IssueSubject::TrackInstance(_) => "track_instance",
        IssueSubject::Library => "library",
    }
}

fn issue_subject_id_to_sql(subject: &IssueSubject) -> Option<String> {
    match subject {
        IssueSubject::Release(id) => Some(id.as_uuid().to_string()),
        IssueSubject::ReleaseInstance(id) => Some(id.as_uuid().to_string()),
        IssueSubject::TrackInstance(id) => Some(id.as_uuid().to_string()),
        IssueSubject::Library => None,
    }
}

fn parse_job_type(value: String) -> JobType {
    match value.as_str() {
        "discover_batch" => JobType::DiscoverBatch,
        "analyze_release_instance" => JobType::AnalyzeReleaseInstance,
        "match_release_instance" => JobType::MatchReleaseInstance,
        "enrich_release_instance" => JobType::EnrichReleaseInstance,
        "render_export_metadata" => JobType::RenderExportMetadata,
        "write_tags" => JobType::WriteTags,
        "organize_files" => JobType::OrganizeFiles,
        "verify_import" => JobType::VerifyImport,
        "reprocess_release_instance" => JobType::ReprocessReleaseInstance,
        _ => JobType::RescanWatcher,
    }
}

fn job_type_to_sql(value: &JobType) -> String {
    match value {
        JobType::DiscoverBatch => "discover_batch",
        JobType::AnalyzeReleaseInstance => "analyze_release_instance",
        JobType::MatchReleaseInstance => "match_release_instance",
        JobType::EnrichReleaseInstance => "enrich_release_instance",
        JobType::RenderExportMetadata => "render_export_metadata",
        JobType::WriteTags => "write_tags",
        JobType::OrganizeFiles => "organize_files",
        JobType::VerifyImport => "verify_import",
        JobType::ReprocessReleaseInstance => "reprocess_release_instance",
        JobType::RescanWatcher => "rescan_watcher",
    }
    .to_string()
}

fn parse_job_status(value: String) -> JobStatus {
    match value.as_str() {
        "queued" => JobStatus::Queued,
        "running" => JobStatus::Running,
        "succeeded" => JobStatus::Succeeded,
        "failed" => JobStatus::Failed,
        _ => JobStatus::Resumable,
    }
}

fn job_status_to_sql(value: &JobStatus) -> String {
    match value {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
        JobStatus::Resumable => "resumable",
    }
    .to_string()
}

fn parse_job_subject(kind: String, id: String) -> Result<JobSubject, String> {
    match kind.as_str() {
        "import_batch" => Ok(JobSubject::ImportBatch(
            ImportBatchId::parse_str(&id).map_err(|error| error.to_string())?,
        )),
        "release_instance" => Ok(JobSubject::ReleaseInstance(
            ReleaseInstanceId::parse_str(&id).map_err(|error| error.to_string())?,
        )),
        "source_scan" => Ok(JobSubject::SourceScan(id)),
        other => Err(format!("unknown job subject kind '{other}'")),
    }
}

fn job_subject_kind_to_sql(subject: &JobSubject) -> &'static str {
    match subject {
        JobSubject::ImportBatch(_) => "import_batch",
        JobSubject::ReleaseInstance(_) => "release_instance",
        JobSubject::SourceScan(_) => "source_scan",
    }
}

fn job_subject_id_to_sql(subject: &JobSubject) -> String {
    match subject {
        JobSubject::ImportBatch(id) => id.as_uuid().to_string(),
        JobSubject::ReleaseInstance(id) => id.as_uuid().to_string(),
        JobSubject::SourceScan(source) => source.clone(),
    }
}

fn parse_job_trigger(value: String) -> JobTrigger {
    match value.as_str() {
        "system" => JobTrigger::System,
        _ => JobTrigger::Operator,
    }
}

fn job_trigger_to_sql(trigger: &JobTrigger) -> &'static str {
    match trigger {
        JobTrigger::System => "system",
        JobTrigger::Operator => "operator",
    }
}

fn parse_qualifier_visibility(value: String) -> QualifierVisibility {
    match value.as_str() {
        "hidden" => QualifierVisibility::Hidden,
        "path_only" => QualifierVisibility::PathOnly,
        _ => QualifierVisibility::TagsAndPath,
    }
}

fn parse_release_id(value: Option<String>) -> Result<ReleaseId, String> {
    ReleaseId::parse_str(&value.ok_or_else(|| "missing release subject id".to_string())?)
        .map_err(|error| error.to_string())
}

fn parse_release_instance_id(value: Option<String>) -> Result<ReleaseInstanceId, String> {
    ReleaseInstanceId::parse_str(
        &value.ok_or_else(|| "missing release instance subject id".to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn parse_track_instance_id(value: Option<String>) -> Result<TrackInstanceId, String> {
    TrackInstanceId::parse_str(
        &value.ok_or_else(|| "missing track instance subject id".to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn invalid_column(column: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(SimpleError(message)))
}

fn storage_error(message: impl Into<String>) -> RepositoryError {
    RepositoryError {
        kind: RepositoryErrorKind::Storage,
        message: message.into(),
    }
}

fn to_storage_error(error: rusqlite::Error) -> RepositoryError {
    storage_error(error.to_string())
}

#[derive(Debug)]
struct SimpleError(String);

impl std::fmt::Display for SimpleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SimpleError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::repository::{
        ExportRepository, IssueRepository, JobCommandRepository, JobRepository,
        ReleaseInstanceRepository, ReleaseRepository,
    };
    use crate::domain::issue::IssueState;
    use crate::domain::job::{JobStatus, JobSubject, JobTrigger, JobType};
    use crate::domain::release_instance::{FormatFamily, ReleaseInstanceState};
    use uuid::Uuid;

    #[test]
    fn repository_context_enables_wal_mode() {
        let (context, _path) = seeded_context();
        let connection = context.read_connection().expect("reader should open");
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode should be queryable");

        assert_eq!(mode.to_lowercase(), "wal");
    }

    #[test]
    fn migrations_apply_and_rollback_cleanly() {
        let database_path =
            std::env::temp_dir().join(format!("discern-test-{}.db", Uuid::new_v4()));
        let context = SqliteRepositoryContext::open(&database_path).expect("context should open");

        context
            .with_write_transaction(|transaction| {
                apply_migrations(transaction)?;
                Ok(())
            })
            .expect("migrations should apply");

        let connection = context.read_connection().expect("reader should open");
        let tables = sqlite_tables(&connection);
        assert!(tables.contains(&"release_instances".to_string()));
        assert!(tables.contains(&"jobs".to_string()));

        connection
            .execute_batch(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/0002_indexes.down.sql"
            )))
            .expect("index rollback should succeed");
        connection
            .execute_batch(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/0001_initial_schema.down.sql"
            )))
            .expect("schema rollback should succeed");

        assert!(sqlite_tables(&connection).is_empty());
    }

    #[test]
    fn migrations_create_expected_indexes() {
        let (context, _path) = seeded_context();
        let connection = context.read_connection().expect("reader should open");
        let release_instance_indexes = sqlite_index_names(&connection, "release_instances");
        let issue_indexes = sqlite_index_names(&connection, "issues");

        assert!(release_instance_indexes.contains(&"idx_release_instances_release_id".to_string()));
        assert!(
            release_instance_indexes.contains(&"idx_release_instances_source_path".to_string())
        );
        assert!(issue_indexes.contains(&"idx_issues_state_type".to_string()));
    }

    #[test]
    fn repositories_return_none_for_missing_records() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let missing_release: ReleaseId = parse_uuid(SeedIds::UNUSED_RELEASE);
        let missing_issue: IssueId = parse_uuid(SeedIds::UNUSED_ISSUE);
        let missing_export: ExportedMetadataSnapshotId = parse_uuid(SeedIds::UNUSED_EXPORT);

        assert_eq!(
            repositories
                .get_release(&missing_release)
                .expect("query should succeed"),
            None
        );
        assert_eq!(
            repositories
                .get_issue(&missing_issue)
                .expect("query should succeed"),
            None
        );
        assert_eq!(
            repositories
                .get_exported_metadata(&missing_export)
                .expect("query should succeed"),
            None
        );
    }

    #[test]
    fn repositories_filter_and_paginate_release_queries() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);

        let release_groups = repositories
            .search_release_groups(&ReleaseGroupSearchQuery {
                text: Some("Rain".to_string()),
                primary_artist_name: Some("Radio".to_string()),
                page: PageRequest::new(1, 0),
            })
            .expect("query should succeed");
        assert_eq!(release_groups.total, 2);
        assert!(release_groups.has_more());
        assert_eq!(release_groups.items.len(), 1);
        assert_eq!(release_groups.items[0].title, "In Rainbows");

        let releases = repositories
            .list_releases(&ReleaseListQuery {
                release_group_id: Some(parse_uuid(SeedIds::RELEASE_GROUP)),
                text: Some("Rain".to_string()),
                page: PageRequest::new(10, 0),
            })
            .expect("query should succeed");
        assert_eq!(releases.total, 1);
        assert_eq!(releases.items[0].title, "In Rainbows");
    }

    #[test]
    fn repositories_filter_and_paginate_candidate_matches() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);

        let page = repositories
            .list_candidate_matches(
                &parse_uuid(SeedIds::RELEASE_INSTANCE),
                &PageRequest::new(1, 0),
            )
            .expect("query should succeed");
        assert_eq!(page.total, 2);
        assert!(page.has_more());
        assert_eq!(page.items[0].provider, CandidateProvider::MusicBrainz);
        assert_eq!(page.items[0].normalized_score.value(), 0.98);
    }

    #[test]
    fn repositories_filter_release_instances_and_exports() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);

        let instances = repositories
            .list_release_instances(&ReleaseInstanceListQuery {
                state: Some(ReleaseInstanceState::Matched),
                format_family: Some(FormatFamily::Flac),
                page: PageRequest::new(1, 0),
                ..ReleaseInstanceListQuery::default()
            })
            .expect("query should succeed");
        assert_eq!(instances.total, 2);
        assert_eq!(
            instances.items[0].technical_variant.format_family,
            FormatFamily::Flac
        );
        assert!(instances.has_more());

        let exported = repositories
            .get_latest_exported_metadata(&parse_uuid(SeedIds::RELEASE_INSTANCE))
            .expect("query should succeed")
            .expect("snapshot should exist");
        assert_eq!(exported.album_title, "In Rainbows [2007 CD]");
        assert!(exported.compatibility.verified);

        let exports = repositories
            .list_exported_metadata(&ExportedMetadataListQuery {
                album_title: Some("Rainbows".to_string()),
                page: PageRequest::new(1, 0),
                ..ExportedMetadataListQuery::default()
            })
            .expect("query should succeed");
        assert_eq!(exports.total, 2);
        assert!(exports.has_more());
    }

    #[test]
    fn repositories_filter_issues_jobs_and_import_batches() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);

        let issues = repositories
            .list_issues(&IssueListQuery {
                state: Some(IssueState::Open),
                ..IssueListQuery::default()
            })
            .expect("query should succeed");
        assert_eq!(issues.total, 1);
        assert_eq!(issues.items[0].summary, "Duplicate import detected");

        let jobs = repositories
            .list_jobs(&JobListQuery {
                status: Some(JobStatus::Queued),
                ..JobListQuery::default()
            })
            .expect("query should succeed");
        assert_eq!(jobs.total, 1);
        assert_eq!(jobs.items[0].progress_phase, "queued");

        let batches = repositories
            .list_import_batches(&ImportBatchListQuery {
                page: PageRequest::new(10, 0),
            })
            .expect("query should succeed");
        assert_eq!(batches.total, 2);
        let batch = repositories
            .get_import_batch(&parse_uuid(SeedIds::IMPORT_BATCH))
            .expect("query should succeed")
            .expect("import batch should exist");
        assert_eq!(
            batch.received_paths,
            vec![PathBuf::from("/incoming/radiohead")]
        );
    }

    #[test]
    fn repositories_persist_job_queue_updates_and_recovery() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let mut job = Job::queued(
            JobType::AnalyzeReleaseInstance,
            JobSubject::SourceScan("startup".to_string()),
            JobTrigger::Operator,
            300,
        );

        repositories
            .create_job(&job)
            .expect("job creation should succeed");
        job.start("analyzing", 301)
            .expect("queued jobs should start");
        repositories
            .update_job(&job)
            .expect("job update should succeed");

        let recoverable = repositories
            .list_recoverable_jobs()
            .expect("recovery query should succeed");
        assert!(recoverable.iter().any(|candidate| candidate.id == job.id));
        assert!(recoverable.iter().all(|candidate| {
            matches!(candidate.status, JobStatus::Queued | JobStatus::Running)
        }));
    }

    fn seeded_context() -> (SqliteRepositoryContext, PathBuf) {
        let database_path =
            std::env::temp_dir().join(format!("discern-test-{}.db", Uuid::new_v4()));
        let context = SqliteRepositoryContext::open(&database_path).expect("context should open");
        context
            .with_write_transaction(|transaction| {
                apply_migrations(transaction)?;
                seed_rows(transaction)?;
                Ok(())
            })
            .expect("seed should succeed");

        (context, database_path)
    }

    fn seed_rows(transaction: &Transaction<'_>) -> Result<(), RepositoryError> {
        transaction
            .execute(
                "INSERT INTO artists (id, name, sort_name, musicbrainz_artist_id)
                 VALUES (?1, ?2, ?3, NULL)",
                params![SeedIds::ARTIST, "Radiohead", "Radiohead"],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO release_groups
                 (id, primary_artist_id, title, normalized_title, kind, musicbrainz_release_group_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    SeedIds::RELEASE_GROUP,
                    SeedIds::ARTIST,
                    "In Rainbows",
                    "in rainbows",
                    "album",
                    SeedIds::MB_RELEASE_GROUP,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO release_groups
                 (id, primary_artist_id, title, normalized_title, kind, musicbrainz_release_group_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
                params![
                    SeedIds::SECOND_RELEASE_GROUP,
                    SeedIds::ARTIST,
                    "Rainbows Live",
                    "rainbows live",
                    "live",
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO releases
                 (id, release_group_id, primary_artist_id, title, normalized_title,
                  musicbrainz_release_id, discogs_release_id, edition_title, disambiguation,
                  country, label, catalog_number, release_year, release_month, release_day)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    SeedIds::RELEASE,
                    SeedIds::RELEASE_GROUP,
                    SeedIds::ARTIST,
                    "In Rainbows",
                    "in rainbows",
                    SeedIds::MB_RELEASE,
                    12345_u64,
                    "2007 CD",
                    "GB",
                    "XL",
                    "XLLP324",
                    2007_i64,
                    12_i64,
                    28_i64,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO releases
                 (id, release_group_id, primary_artist_id, title, normalized_title,
                  musicbrainz_release_id, discogs_release_id, edition_title, disambiguation,
                  country, label, catalog_number, release_year, release_month, release_day)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'Live set', ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    SeedIds::SECOND_RELEASE,
                    SeedIds::SECOND_RELEASE_GROUP,
                    SeedIds::ARTIST,
                    "Rainbows Live",
                    "rainbows live",
                    SeedIds::SECOND_MB_RELEASE,
                    54321_u64,
                    "2018 Digital",
                    "GB",
                    "Self Released",
                    "DIGI001",
                    2018_i64,
                    6_i64,
                    1_i64,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO sources
                 (id, kind, display_name, locator_kind, locator_value, external_reference)
                 VALUES (?1, 'watch_directory', 'Incoming', 'filesystem_path', '/incoming', NULL)",
                params![SeedIds::SOURCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO import_batches
                 (id, source_id, mode, status, requested_by_kind, requested_by_name, created_at_unix_seconds)
                 VALUES (?1, ?2, 'copy', 'submitted', 'system', NULL, 100)",
                params![SeedIds::IMPORT_BATCH, SeedIds::SOURCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO import_batch_paths (import_batch_id, ordinal, path)
                 VALUES (?1, 0, '/incoming/radiohead')",
                params![SeedIds::IMPORT_BATCH],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO import_batches
                 (id, source_id, mode, status, requested_by_kind, requested_by_name, created_at_unix_seconds)
                 VALUES (?1, ?2, 'hardlink', 'created', 'external_client', 'api', 90)",
                params![SeedIds::SECOND_IMPORT_BATCH, SeedIds::SOURCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO import_batch_paths (import_batch_id, ordinal, path)
                 VALUES (?1, 0, '/incoming/radiohead-live')",
                params![SeedIds::SECOND_IMPORT_BATCH],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO release_instances
                 (id, release_id, source_id, state, format_family, bitrate_mode, bitrate_kbps,
                  sample_rate_hz, bit_depth, track_count, total_duration_seconds, ingest_origin,
                  import_mode, duplicate_status, export_visibility_policy, original_source_path,
                  imported_at_unix_seconds, gazelle_tracker, gazelle_torrent_id, gazelle_release_group_id)
                 VALUES (?1, ?2, ?3, 'matched', 'flac', 'lossless', NULL, 44100, 16,
                         10, 2550, 'watch_directory', 'copy', NULL, NULL,
                         '/incoming/radiohead/In Rainbows', 120, 'redacted', '999', '555')",
                params![SeedIds::RELEASE_INSTANCE, SeedIds::RELEASE, SeedIds::SOURCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO release_instances
                 (id, release_id, source_id, state, format_family, bitrate_mode, bitrate_kbps,
                  sample_rate_hz, bit_depth, track_count, total_duration_seconds, ingest_origin,
                  import_mode, duplicate_status, export_visibility_policy, original_source_path,
                  imported_at_unix_seconds, gazelle_tracker, gazelle_torrent_id, gazelle_release_group_id)
                 VALUES (?1, ?2, ?3, 'matched', 'flac', 'constant', 320, 48000, 24,
                         8, 2100, 'manual_add', 'hardlink', NULL, NULL,
                         '/incoming/radiohead-live/Rainbows Live', 220, NULL, NULL, NULL)",
                params![
                    SeedIds::SECOND_RELEASE_INSTANCE,
                    SeedIds::SECOND_RELEASE,
                    SeedIds::SOURCE
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO candidate_matches
                 (id, release_instance_id, provider, candidate_kind, provider_entity_id,
                  normalized_score, evidence_matches_json, mismatches_json,
                  unresolved_ambiguities_json, provider_provenance_json, created_at_unix_seconds)
                 VALUES (?1, ?2, 'musicbrainz', 'release', ?3, ?4, ?5, ?6, ?7, ?8, 125)",
                params![
                    SeedIds::CANDIDATE_MATCH,
                    SeedIds::RELEASE_INSTANCE,
                    SeedIds::MB_RELEASE,
                    0.98_f64,
                    r#"[{"kind":"artist_match","detail":"artist names aligned"}]"#,
                    r#"[]"#,
                    r#"["vinyl reissue also matched"]"#,
                    r#"{"provider_name":"musicbrainz","query":"in rainbows","fetched_at_unix_seconds":124}"#,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO candidate_matches
                 (id, release_instance_id, provider, candidate_kind, provider_entity_id,
                  normalized_score, evidence_matches_json, mismatches_json,
                  unresolved_ambiguities_json, provider_provenance_json, created_at_unix_seconds)
                 VALUES (?1, ?2, 'discogs', 'release_group', 'discogs-rainbows-live', ?3, ?4, ?5, ?6, ?7, 225)",
                params![
                    SeedIds::SECOND_CANDIDATE_MATCH,
                    SeedIds::RELEASE_INSTANCE,
                    0.67_f64,
                    r#"[{"kind":"filename_similarity","detail":"folder name loosely matched"}]"#,
                    r#"[{"kind":"track_count_match","detail":"track count differs"}]"#,
                    r#"[]"#,
                    r#"{"provider_name":"discogs","query":"rainbows live","fetched_at_unix_seconds":224}"#,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO exported_metadata_snapshots
                 (id, release_instance_id, export_profile, album_title, album_artist,
                  artist_credits_json, edition_visibility, technical_visibility,
                  path_components_json, primary_artwork_filename, compatibility_verified,
                  compatibility_warnings_json, rendered_at_unix_seconds)
                 VALUES (?1, ?2, 'generic_player', 'In Rainbows [2007 CD]', 'Radiohead',
                         ?3, 'tags_and_path', 'path_only', ?4, 'cover.jpg', 1, ?5, 130)",
                params![
                    SeedIds::EXPORTED_METADATA,
                    SeedIds::RELEASE_INSTANCE,
                    r#"["Radiohead"]"#,
                    r#"["Radiohead","In Rainbows","2007 CD"]"#,
                    r#"[]"#,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO exported_metadata_snapshots
                 (id, release_instance_id, export_profile, album_title, album_artist,
                  artist_credits_json, edition_visibility, technical_visibility,
                  path_components_json, primary_artwork_filename, compatibility_verified,
                  compatibility_warnings_json, rendered_at_unix_seconds)
                 VALUES (?1, ?2, 'generic_player', 'Rainbows Live [2018 Digital]', 'Radiohead',
                         ?3, 'tags_and_path', 'path_only', ?4, NULL, 0, ?5, 230)",
                params![
                    SeedIds::SECOND_EXPORTED_METADATA,
                    SeedIds::SECOND_RELEASE_INSTANCE,
                    r#"["Radiohead"]"#,
                    r#"["Radiohead","Rainbows Live","2018 Digital"]"#,
                    r#"["artwork missing"]"#,
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO issues
                 (id, issue_type, state, subject_kind, subject_id, summary, details,
                  created_at_unix_seconds, resolved_at_unix_seconds, suppressed_reason)
                 VALUES (?1, 'duplicate_release_instance', 'open', 'release_instance', ?2,
                         'Duplicate import detected', 'Review required', 140, NULL, NULL)",
                params![SeedIds::ISSUE, SeedIds::RELEASE_INSTANCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO issues
                 (id, issue_type, state, subject_kind, subject_id, summary, details,
                  created_at_unix_seconds, resolved_at_unix_seconds, suppressed_reason)
                 VALUES (?1, 'missing_artwork', 'resolved', 'release_instance', ?2,
                         'Artwork backfilled', 'Handled manually', 141, 160, NULL)",
                params![SeedIds::SECOND_ISSUE, SeedIds::SECOND_RELEASE_INSTANCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO jobs
                 (id, job_type, subject_kind, subject_id, status, progress_phase,
                  retry_count, triggered_by, created_at_unix_seconds,
                  started_at_unix_seconds, finished_at_unix_seconds, error_payload)
                 VALUES (?1, 'match_release_instance', 'release_instance', ?2, 'queued',
                         'queued', 0, 'system', 150, NULL, NULL, NULL)",
                params![SeedIds::JOB, SeedIds::RELEASE_INSTANCE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO jobs
                 (id, job_type, subject_kind, subject_id, status, progress_phase,
                  retry_count, triggered_by, created_at_unix_seconds,
                  started_at_unix_seconds, finished_at_unix_seconds, error_payload)
                 VALUES (?1, 'verify_import', 'release_instance', ?2, 'running',
                         'compatibility', 1, 'operator', 151, 152, NULL, NULL)",
                params![SeedIds::SECOND_JOB, SeedIds::SECOND_RELEASE_INSTANCE],
            )
            .map_err(to_storage_error)?;

        Ok(())
    }

    fn apply_migrations(transaction: &Transaction<'_>) -> Result<(), RepositoryError> {
        transaction
            .execute_batch(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/0001_initial_schema.up.sql"
            )))
            .map_err(to_storage_error)?;
        transaction
            .execute_batch(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/migrations/0002_indexes.up.sql"
            )))
            .map_err(to_storage_error)?;
        Ok(())
    }

    fn sqlite_tables(connection: &Connection) -> Vec<String> {
        let mut statement = connection
            .prepare(
                "SELECT name
                 FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                 ORDER BY name ASC",
            )
            .expect("table query should prepare");
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("table query should run")
            .collect::<Result<Vec<_>, _>>()
            .expect("table rows should parse")
    }

    fn sqlite_index_names(connection: &Connection, table: &str) -> Vec<String> {
        let mut statement = connection
            .prepare(&format!("PRAGMA index_list('{table}')"))
            .expect("index query should prepare");
        statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("index query should run")
            .collect::<Result<Vec<_>, _>>()
            .expect("index rows should parse")
    }

    fn parse_uuid<T>(value: &str) -> T
    where
        T: ParseUuidId,
    {
        T::parse(value).expect("id should parse")
    }

    struct SeedIds;

    impl SeedIds {
        const ARTIST: &str = "11111111-1111-1111-1111-111111111111";
        const RELEASE_GROUP: &str = "22222222-2222-2222-2222-222222222222";
        const SECOND_RELEASE_GROUP: &str = "23232323-2323-2323-2323-232323232323";
        const RELEASE: &str = "33333333-3333-3333-3333-333333333333";
        const SECOND_RELEASE: &str = "34343434-3434-3434-3434-343434343434";
        const SOURCE: &str = "44444444-4444-4444-4444-444444444444";
        const IMPORT_BATCH: &str = "55555555-5555-5555-5555-555555555555";
        const SECOND_IMPORT_BATCH: &str = "56565656-5656-5656-5656-565656565656";
        const RELEASE_INSTANCE: &str = "66666666-6666-6666-6666-666666666666";
        const SECOND_RELEASE_INSTANCE: &str = "67676767-6767-6767-6767-676767676767";
        const CANDIDATE_MATCH: &str = "77777777-7777-7777-7777-777777777777";
        const SECOND_CANDIDATE_MATCH: &str = "78787878-7878-7878-7878-787878787878";
        const EXPORTED_METADATA: &str = "88888888-8888-8888-8888-888888888888";
        const SECOND_EXPORTED_METADATA: &str = "89898989-8989-8989-8989-898989898989";
        const ISSUE: &str = "99999999-9999-9999-9999-999999999999";
        const SECOND_ISSUE: &str = "9a9a9a9a-9a9a-9a9a-9a9a-9a9a9a9a9a9a";
        const JOB: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        const SECOND_JOB: &str = "abababab-abab-abab-abab-abababababab";
        const MB_RELEASE_GROUP: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        const MB_RELEASE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
        const SECOND_MB_RELEASE: &str = "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd";
        const UNUSED_RELEASE: &str = "dededede-dede-dede-dede-dededededede";
        const UNUSED_ISSUE: &str = "efefefef-efef-efef-efef-efefefefefef";
        const UNUSED_EXPORT: &str = "f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0";
    }
}
