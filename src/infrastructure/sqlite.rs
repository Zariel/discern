use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

use crate::application::repository::{
    ExportCommandRepository, ExportRepository, ExportedMetadataListQuery,
    ImportBatchCommandRepository, ImportBatchListQuery, ImportBatchRepository,
    IngestEvidenceCommandRepository, IngestEvidenceRepository, IssueCommandRepository,
    IssueListQuery, IssueRepository, JobCommandRepository, JobListQuery, JobRepository,
    ManualOverrideCommandRepository, ManualOverrideListQuery, ManualOverrideRepository,
    MetadataSnapshotCommandRepository, MetadataSnapshotRepository, ReleaseCommandRepository,
    ReleaseGroupSearchQuery, ReleaseInstanceCommandRepository, ReleaseInstanceListQuery,
    ReleaseInstanceRepository, ReleaseListQuery, ReleaseRepository, RepositoryError,
    RepositoryErrorKind, SourceCommandRepository, SourceRepository,
    StagingManifestCommandRepository, StagingManifestRepository,
};
use crate::domain::artist::Artist;
use crate::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
    EvidenceNote, ProviderProvenance,
};
use crate::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use crate::domain::file::{FileRecord, FileRole};
use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
use crate::domain::ingest_evidence::{
    IngestEvidenceRecord, IngestEvidenceSource, IngestEvidenceSubject, ObservedValue,
    ObservedValueKind,
};
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType};
use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
use crate::domain::metadata_snapshot::{
    MetadataSnapshot, MetadataSnapshotSource, MetadataSubject, SnapshotFormat,
};
use crate::domain::release::{PartialDate, Release, ReleaseEdition};
use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use crate::domain::release_instance::{
    BitrateMode, FormatFamily, GazelleReference, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
    ReleaseInstanceState, TechnicalVariant,
};
use crate::domain::source::{Source, SourceKind, SourceLocator};
use crate::domain::staging_manifest::{
    AuxiliaryFile, AuxiliaryFileRole, FileFingerprint, GroupingDecision, GroupingStrategy,
    ObservedTag, StagedFile, StagedReleaseGroup, StagingManifest, StagingManifestSource,
};
use crate::domain::track::{Track, TrackPosition};
use crate::domain::track_instance::{AudioProperties, TrackInstance};
use crate::support::ids::{
    ArtistId, CandidateMatchId, DiscogsReleaseId, ExportedMetadataSnapshotId, FileId,
    ImportBatchId, IngestEvidenceId, IssueId, JobId, ManualOverrideId, MetadataSnapshotId,
    MusicBrainzReleaseGroupId, MusicBrainzReleaseId, ReleaseGroupId, ReleaseId, ReleaseInstanceId,
    SourceId, StagingManifestId, TrackId, TrackInstanceId,
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
    fn find_artist_by_musicbrainz_id(
        &self,
        musicbrainz_artist_id: &str,
    ) -> Result<Option<Artist>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, name, sort_name, musicbrainz_artist_id
                 FROM artists
                 WHERE musicbrainz_artist_id = ?1",
                params![musicbrainz_artist_id],
                map_artist,
            )
            .optional()
            .map_err(to_storage_error)
    }

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

    fn find_release_group_by_musicbrainz_id(
        &self,
        musicbrainz_release_group_id: &str,
    ) -> Result<Option<ReleaseGroup>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, primary_artist_id, title, kind, musicbrainz_release_group_id
                 FROM release_groups
                 WHERE musicbrainz_release_group_id = ?1",
                params![musicbrainz_release_group_id],
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

    fn list_tracks_for_release(
        &self,
        release_id: &ReleaseId,
    ) -> Result<Vec<Track>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, release_id, disc_number, track_number, title,
                        musicbrainz_track_id, duration_ms
                 FROM tracks
                 WHERE release_id = ?1
                 ORDER BY disc_number ASC, track_number ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(params![release_id.as_uuid().to_string()], map_track)
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }
}

impl ReleaseCommandRepository for SqliteRepositories {
    fn create_artist(&self, artist: &Artist) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO artists (id, name, sort_name, musicbrainz_artist_id)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        artist.id.as_uuid().to_string(),
                        &artist.name,
                        &artist.sort_name,
                        artist
                            .musicbrainz_artist_id
                            .as_ref()
                            .map(|value| value.as_uuid().to_string()),
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }

    fn create_release_group(&self, release_group: &ReleaseGroup) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO release_groups
                     (id, primary_artist_id, title, normalized_title, kind, musicbrainz_release_group_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        release_group.id.as_uuid().to_string(),
                        release_group.primary_artist_id.as_uuid().to_string(),
                        &release_group.title,
                        release_group.title.to_lowercase(),
                        release_group_kind_to_sql(&release_group.kind),
                        release_group
                            .musicbrainz_release_group_id
                            .as_ref()
                            .map(|value| value.as_uuid().to_string()),
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }

    fn create_release(&self, release: &Release) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO releases
                     (id, release_group_id, primary_artist_id, title, normalized_title,
                      musicbrainz_release_id, discogs_release_id, edition_title, disambiguation,
                      country, label, catalog_number, release_year, release_month, release_day)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                    params![
                        release.id.as_uuid().to_string(),
                        release.release_group_id.as_uuid().to_string(),
                        release.primary_artist_id.as_uuid().to_string(),
                        &release.title,
                        release.title.to_lowercase(),
                        release
                            .musicbrainz_release_id
                            .as_ref()
                            .map(|value| value.as_uuid().to_string()),
                        release
                            .discogs_release_id
                            .as_ref()
                            .map(|value| value.value()),
                        &release.edition.edition_title,
                        &release.edition.disambiguation,
                        &release.edition.country,
                        &release.edition.label,
                        &release.edition.catalog_number,
                        release
                            .edition
                            .release_date
                            .as_ref()
                            .map(|value| i64::from(value.year)),
                        release
                            .edition
                            .release_date
                            .as_ref()
                            .and_then(|value| value.month.map(i64::from)),
                        release
                            .edition
                            .release_date
                            .as_ref()
                            .and_then(|value| value.day.map(i64::from)),
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
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
                "SELECT id, import_batch_id, source_id, release_id, state, format_family, bitrate_mode,
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
                "SELECT id, import_batch_id, source_id, release_id, state, format_family, bitrate_mode,
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

    fn list_release_instances_for_batch(
        &self,
        import_batch_id: &ImportBatchId,
    ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, import_batch_id, source_id, release_id, state, format_family, bitrate_mode,
                        bitrate_kbps, sample_rate_hz, bit_depth, track_count,
                        total_duration_seconds, ingest_origin, original_source_path,
                        imported_at_unix_seconds, gazelle_tracker, gazelle_torrent_id,
                        gazelle_release_group_id
                 FROM release_instances
                 WHERE import_batch_id = ?1
                 ORDER BY imported_at_unix_seconds DESC, id ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![import_batch_id.as_uuid().to_string()],
                map_release_instance,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
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

    fn list_track_instances_for_release_instance(
        &self,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<Vec<TrackInstance>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, release_instance_id, track_id, observed_disc_number,
                        observed_track_number, observed_title, format_family,
                        duration_ms, bitrate_kbps, sample_rate_hz, bit_depth
                 FROM track_instances
                 WHERE release_instance_id = ?1
                 ORDER BY observed_disc_number ASC, observed_track_number ASC, id ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![release_instance_id.as_uuid().to_string()],
                map_track_instance,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }

    fn list_files_for_release_instance(
        &self,
        release_instance_id: &ReleaseInstanceId,
        role: Option<FileRole>,
    ) -> Result<Vec<FileRecord>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let role = role.as_ref().map(file_role_to_sql);
        let mut statement = connection
            .prepare(
                "SELECT files.id, files.track_instance_id, files.role, files.format_family,
                        files.path, files.checksum, files.size_bytes
                 FROM files
                 INNER JOIN track_instances
                         ON track_instances.id = files.track_instance_id
                 WHERE track_instances.release_instance_id = ?1
                   AND (?2 IS NULL OR files.role = ?2)
                 ORDER BY files.path ASC, files.id ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![release_instance_id.as_uuid().to_string(), role.as_deref()],
                map_file_record,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }
}

impl ReleaseInstanceCommandRepository for SqliteRepositories {
    fn create_release_instance(
        &self,
        release_instance: &ReleaseInstance,
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            write_release_instance(transaction, release_instance)?;
            Ok(())
        })
    }

    fn update_release_instance(
        &self,
        release_instance: &ReleaseInstance,
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "UPDATE release_instances
                     SET import_batch_id = ?2,
                         source_id = ?3,
                         release_id = ?4,
                         state = ?5,
                         format_family = ?6,
                         bitrate_mode = ?7,
                         bitrate_kbps = ?8,
                         sample_rate_hz = ?9,
                         bit_depth = ?10,
                         track_count = ?11,
                         total_duration_seconds = ?12,
                         ingest_origin = ?13,
                         original_source_path = ?14,
                         imported_at_unix_seconds = ?15,
                         gazelle_tracker = ?16,
                         gazelle_torrent_id = ?17,
                         gazelle_release_group_id = ?18
                     WHERE id = ?1",
                    params![
                        release_instance.id.as_uuid().to_string(),
                        release_instance.import_batch_id.as_uuid().to_string(),
                        release_instance.source_id.as_uuid().to_string(),
                        release_instance
                            .release_id
                            .as_ref()
                            .map(|value| value.as_uuid().to_string()),
                        release_instance_state_to_sql(&release_instance.state),
                        format_family_to_sql(&release_instance.technical_variant.format_family),
                        bitrate_mode_to_sql(&release_instance.technical_variant.bitrate_mode),
                        release_instance
                            .technical_variant
                            .bitrate_kbps
                            .map(i64::from),
                        release_instance
                            .technical_variant
                            .sample_rate_hz
                            .map(i64::from),
                        release_instance.technical_variant.bit_depth.map(i64::from),
                        i64::from(release_instance.technical_variant.track_count),
                        i64::from(release_instance.technical_variant.total_duration_seconds),
                        ingest_origin_to_sql(&release_instance.provenance.ingest_origin),
                        &release_instance.provenance.original_source_path,
                        release_instance.provenance.imported_at_unix_seconds,
                        release_instance
                            .provenance
                            .gazelle_reference
                            .as_ref()
                            .map(|value| value.tracker.clone()),
                        release_instance
                            .provenance
                            .gazelle_reference
                            .as_ref()
                            .and_then(|value| value.torrent_id.clone()),
                        release_instance
                            .provenance
                            .gazelle_reference
                            .as_ref()
                            .and_then(|value| value.release_group_id.clone()),
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }

    fn replace_candidate_matches(
        &self,
        release_instance_id: &ReleaseInstanceId,
        matches: &[CandidateMatch],
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "DELETE FROM candidate_matches
                     WHERE release_instance_id = ?1",
                    params![release_instance_id.as_uuid().to_string()],
                )
                .map_err(to_storage_error)?;
            for candidate_match in matches {
                transaction
                    .execute(
                        "INSERT INTO candidate_matches
                         (id, release_instance_id, provider, candidate_kind, provider_entity_id,
                          normalized_score, evidence_matches_json, mismatches_json,
                          unresolved_ambiguities_json, provider_provenance_json,
                          created_at_unix_seconds)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                        params![
                            candidate_match.id.as_uuid().to_string(),
                            candidate_match.release_instance_id.as_uuid().to_string(),
                            candidate_provider_to_sql(&candidate_match.provider),
                            candidate_subject_kind_to_sql(&candidate_match.subject),
                            candidate_subject_id(&candidate_match.subject),
                            f64::from(candidate_match.normalized_score.value()),
                            serialize_evidence_notes(&candidate_match.evidence_matches)
                                .map_err(storage_error)?,
                            serialize_evidence_notes(&candidate_match.mismatches)
                                .map_err(storage_error)?,
                            serde_json::to_string(&candidate_match.unresolved_ambiguities)
                                .map_err(|error| storage_error(error.to_string()))?,
                            serialize_provider_provenance(&candidate_match.provider_provenance)
                                .map_err(storage_error)?,
                            candidate_match.provider_provenance.fetched_at_unix_seconds,
                        ],
                    )
                    .map_err(to_storage_error)?;
            }
            Ok(())
        })
    }

    fn replace_candidate_matches_for_provider(
        &self,
        release_instance_id: &ReleaseInstanceId,
        provider: &CandidateProvider,
        matches: &[CandidateMatch],
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "DELETE FROM candidate_matches
                     WHERE release_instance_id = ?1
                       AND provider = ?2",
                    params![
                        release_instance_id.as_uuid().to_string(),
                        candidate_provider_to_sql(provider),
                    ],
                )
                .map_err(to_storage_error)?;
            for candidate_match in matches {
                transaction
                    .execute(
                        "INSERT INTO candidate_matches
                         (id, release_instance_id, provider, candidate_kind, provider_entity_id,
                          normalized_score, evidence_matches_json, mismatches_json,
                          unresolved_ambiguities_json, provider_provenance_json,
                          created_at_unix_seconds)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                        params![
                            candidate_match.id.as_uuid().to_string(),
                            candidate_match.release_instance_id.as_uuid().to_string(),
                            candidate_provider_to_sql(&candidate_match.provider),
                            candidate_subject_kind_to_sql(&candidate_match.subject),
                            candidate_subject_id(&candidate_match.subject),
                            f64::from(candidate_match.normalized_score.value()),
                            serialize_evidence_notes(&candidate_match.evidence_matches)
                                .map_err(storage_error)?,
                            serialize_evidence_notes(&candidate_match.mismatches)
                                .map_err(storage_error)?,
                            serde_json::to_string(&candidate_match.unresolved_ambiguities)
                                .map_err(|error| storage_error(error.to_string()))?,
                            serialize_provider_provenance(&candidate_match.provider_provenance)
                                .map_err(storage_error)?,
                            candidate_match.provider_provenance.fetched_at_unix_seconds,
                        ],
                    )
                    .map_err(to_storage_error)?;
            }
            Ok(())
        })
    }

    fn replace_track_instances_and_files(
        &self,
        release_instance_id: &ReleaseInstanceId,
        track_instances: &[TrackInstance],
        files: &[FileRecord],
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "DELETE FROM files
                     WHERE track_instance_id IN (
                        SELECT id FROM track_instances WHERE release_instance_id = ?1
                     )",
                    params![release_instance_id.as_uuid().to_string()],
                )
                .map_err(to_storage_error)?;
            transaction
                .execute(
                    "DELETE FROM track_instances
                     WHERE release_instance_id = ?1",
                    params![release_instance_id.as_uuid().to_string()],
                )
                .map_err(to_storage_error)?;
            for track_instance in track_instances {
                write_track_instance(transaction, track_instance)?;
            }
            for file in files {
                write_file_record(transaction, file)?;
            }
            Ok(())
        })
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
    fn get_source(&self, id: &SourceId) -> Result<Option<Source>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, kind, display_name, locator_kind, locator_value, external_reference
                 FROM sources
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_source,
            )
            .optional()
            .map_err(to_storage_error)
    }

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

    fn update_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "UPDATE import_batches
                     SET source_id = ?2,
                         mode = ?3,
                         status = ?4,
                         requested_by_kind = ?5,
                         requested_by_name = ?6,
                         created_at_unix_seconds = ?7
                     WHERE id = ?1",
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

            transaction
                .execute(
                    "DELETE FROM import_batch_paths
                     WHERE import_batch_id = ?1",
                    params![batch.id.as_uuid().to_string()],
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

impl StagingManifestRepository for SqliteRepositories {
    fn list_staging_manifests_for_batch(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<Vec<StagingManifest>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, batch_id, source_kind, source_path, discovered_files_json,
                        auxiliary_files_json, grouping_strategy, grouping_groups_json,
                        grouping_notes_json, captured_at_unix_seconds
                 FROM staging_manifests
                 WHERE batch_id = ?1
                 ORDER BY captured_at_unix_seconds DESC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![batch_id.as_uuid().to_string()],
                map_staging_manifest,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }
}

impl StagingManifestCommandRepository for SqliteRepositories {
    fn create_staging_manifest(&self, manifest: &StagingManifest) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO staging_manifests
                     (id, batch_id, source_kind, source_path, discovered_files_json,
                      auxiliary_files_json, grouping_strategy, grouping_groups_json,
                      grouping_notes_json, captured_at_unix_seconds)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        manifest.id.as_uuid().to_string(),
                        manifest.batch_id.as_uuid().to_string(),
                        source_kind_to_sql(&manifest.source.kind),
                        manifest.source.source_path.to_string_lossy().to_string(),
                        serialize_staged_files(&manifest.discovered_files)?,
                        serialize_auxiliary_files(&manifest.auxiliary_files)?,
                        grouping_strategy_to_sql(&manifest.grouping.strategy),
                        serialize_staged_release_groups(&manifest.grouping.groups)?,
                        serialize_string_array(&manifest.grouping.notes)?,
                        manifest.captured_at_unix_seconds,
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }
}

impl IngestEvidenceRepository for SqliteRepositories {
    fn list_ingest_evidence_for_batch(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<Vec<IngestEvidenceRecord>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, batch_id, subject_kind, subject_value, source,
                        observations_json, structured_payload, captured_at_unix_seconds
                 FROM ingest_evidence_records
                 WHERE batch_id = ?1
                 ORDER BY captured_at_unix_seconds DESC, id ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![batch_id.as_uuid().to_string()],
                map_ingest_evidence_record,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }
}

impl IngestEvidenceCommandRepository for SqliteRepositories {
    fn create_ingest_evidence_records(
        &self,
        records: &[IngestEvidenceRecord],
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            for record in records {
                let (subject_kind, subject_value) = ingest_evidence_subject_to_sql(&record.subject);
                transaction
                    .execute(
                        "INSERT INTO ingest_evidence_records
                         (id, batch_id, subject_kind, subject_value, source,
                          observations_json, structured_payload, captured_at_unix_seconds)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            record.id.as_uuid().to_string(),
                            record.batch_id.as_uuid().to_string(),
                            subject_kind,
                            subject_value,
                            ingest_evidence_source_to_sql(&record.source),
                            serialize_observed_values(&record.observations)?,
                            &record.structured_payload,
                            record.captured_at_unix_seconds,
                        ],
                    )
                    .map_err(to_storage_error)?;
            }
            Ok(())
        })
    }
}

impl MetadataSnapshotRepository for SqliteRepositories {
    fn list_metadata_snapshots_for_batch(
        &self,
        batch_id: &ImportBatchId,
    ) -> Result<Vec<MetadataSnapshot>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, subject_kind, subject_id, source, format, payload,
                        captured_at_unix_seconds
                 FROM metadata_snapshots
                 WHERE subject_kind = 'import_batch' AND subject_id = ?1
                 ORDER BY captured_at_unix_seconds DESC, id ASC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![batch_id.as_uuid().to_string()],
                map_metadata_snapshot,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }

    fn list_metadata_snapshots_for_release_instance(
        &self,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<Vec<MetadataSnapshot>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, subject_kind, subject_id, source, format, payload,
                        captured_at_unix_seconds
                 FROM metadata_snapshots
                 WHERE subject_kind = 'release_instance'
                   AND subject_id = ?1
                 ORDER BY captured_at_unix_seconds DESC",
            )
            .map_err(to_storage_error)?;
        statement
            .query_map(
                params![release_instance_id.as_uuid().to_string()],
                map_metadata_snapshot,
            )
            .map_err(to_storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(to_storage_error)
    }
}

impl MetadataSnapshotCommandRepository for SqliteRepositories {
    fn create_metadata_snapshots(
        &self,
        snapshots: &[MetadataSnapshot],
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            for snapshot in snapshots {
                let (subject_kind, subject_id) = metadata_subject_to_sql(&snapshot.subject);
                transaction
                    .execute(
                        "INSERT INTO metadata_snapshots
                         (id, subject_kind, subject_id, source, format, payload,
                          captured_at_unix_seconds)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            snapshot.id.as_uuid().to_string(),
                            subject_kind,
                            subject_id,
                            metadata_snapshot_source_to_sql(&snapshot.source),
                            snapshot_format_to_sql(&snapshot.format),
                            &snapshot.payload,
                            snapshot.captured_at_unix_seconds,
                        ],
                    )
                    .map_err(to_storage_error)?;
            }
            Ok(())
        })
    }
}

impl ManualOverrideRepository for SqliteRepositories {
    fn get_manual_override(
        &self,
        id: &ManualOverrideId,
    ) -> Result<Option<ManualOverride>, RepositoryError> {
        let connection = self.context.read_connection()?;
        connection
            .query_row(
                "SELECT id, subject_kind, subject_id, field, value, note,
                        created_by, created_at_unix_seconds
                 FROM manual_overrides
                 WHERE id = ?1",
                params![id.as_uuid().to_string()],
                map_manual_override,
            )
            .optional()
            .map_err(to_storage_error)
    }

    fn list_manual_overrides(
        &self,
        query: &ManualOverrideListQuery,
    ) -> Result<Page<ManualOverride>, RepositoryError> {
        let connection = self.context.read_connection()?;
        let subject_kind = query
            .subject
            .as_ref()
            .map(manual_override_subject_kind_to_sql);
        let subject_id = query
            .subject
            .as_ref()
            .map(manual_override_subject_id_to_sql);
        let field = query.field.as_ref().map(manual_override_field_to_sql);
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM manual_overrides
                 WHERE (?1 IS NULL OR subject_kind = ?1)
                   AND (?2 IS NULL OR subject_id = ?2)
                   AND (?3 IS NULL OR field = ?3)",
                params![subject_kind, subject_id, field],
                |row| row.get(0),
            )
            .map_err(to_storage_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, subject_kind, subject_id, field, value, note,
                        created_by, created_at_unix_seconds
                 FROM manual_overrides
                 WHERE (?1 IS NULL OR subject_kind = ?1)
                   AND (?2 IS NULL OR subject_id = ?2)
                   AND (?3 IS NULL OR field = ?3)
                 ORDER BY created_at_unix_seconds DESC
                 LIMIT ?4 OFFSET ?5",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    subject_kind,
                    subject_id,
                    field,
                    i64::from(query.page.limit),
                    query.page.offset as i64,
                ],
                map_manual_override,
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

impl ManualOverrideCommandRepository for SqliteRepositories {
    fn create_manual_override(
        &self,
        override_record: &ManualOverride,
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO manual_overrides
                     (id, subject_kind, subject_id, field, value, note, created_by, created_at_unix_seconds)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        override_record.id.as_uuid().to_string(),
                        manual_override_subject_kind_to_sql(&override_record.subject),
                        manual_override_subject_id_to_sql(&override_record.subject),
                        manual_override_field_to_sql(&override_record.field),
                        &override_record.value,
                        &override_record.note,
                        &override_record.created_by,
                        override_record.created_at_unix_seconds,
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
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
        let subject_kind = query.subject.as_ref().map(issue_subject_kind_to_sql);
        let subject_id = query.subject.as_ref().and_then(issue_subject_id_to_sql);
        let total: i64 = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM issues
                 WHERE (?1 IS NULL OR state = ?1)
                   AND (?2 IS NULL OR issue_type = ?2)
                   AND (?3 IS NULL OR subject_kind = ?3)
                   AND (?4 IS NULL OR subject_id = ?4)",
                params![
                    state.as_deref(),
                    issue_type.as_deref(),
                    subject_kind,
                    subject_id,
                ],
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
                   AND (?3 IS NULL OR subject_kind = ?3)
                   AND (?4 IS NULL OR subject_id = ?4)
                 ORDER BY created_at_unix_seconds DESC
                 LIMIT ?5 OFFSET ?6",
            )
            .map_err(to_storage_error)?;
        let items = statement
            .query_map(
                params![
                    state.as_deref(),
                    issue_type.as_deref(),
                    subject_kind,
                    subject_id,
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

impl ExportCommandRepository for SqliteRepositories {
    fn create_exported_metadata_snapshot(
        &self,
        snapshot: &ExportedMetadataSnapshot,
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            transaction
                .execute(
                    "INSERT INTO exported_metadata_snapshots
                     (id, release_instance_id, export_profile, album_title, album_artist,
                      artist_credits_json, edition_visibility, technical_visibility,
                      path_components_json, primary_artwork_filename, compatibility_verified,
                      compatibility_warnings_json, rendered_at_unix_seconds)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        snapshot.id.as_uuid().to_string(),
                        snapshot.release_instance_id.as_uuid().to_string(),
                        &snapshot.export_profile,
                        &snapshot.album_title,
                        &snapshot.album_artist,
                        serde_json::to_string(&snapshot.artist_credits).map_err(|error| {
                            storage_error(format!(
                                "failed to encode exported artist credits: {error}"
                            ))
                        })?,
                        qualifier_visibility_to_sql(&snapshot.edition_visibility),
                        qualifier_visibility_to_sql(&snapshot.technical_visibility),
                        serde_json::to_string(&snapshot.path_components).map_err(|error| {
                            storage_error(format!(
                                "failed to encode exported path components: {error}"
                            ))
                        })?,
                        &snapshot.primary_artwork_filename,
                        snapshot.compatibility.verified,
                        serde_json::to_string(&snapshot.compatibility.warnings).map_err(
                            |error| {
                                storage_error(format!(
                                    "failed to encode exported compatibility warnings: {error}"
                                ))
                            },
                        )?,
                        snapshot.rendered_at_unix_seconds,
                    ],
                )
                .map_err(to_storage_error)?;
            Ok(())
        })
    }

    fn update_exported_metadata_snapshot(
        &self,
        snapshot: &ExportedMetadataSnapshot,
    ) -> Result<(), RepositoryError> {
        self.context.with_write_transaction(|transaction| {
            let changed = transaction
                .execute(
                    "UPDATE exported_metadata_snapshots
                     SET release_instance_id = ?2,
                         export_profile = ?3,
                         album_title = ?4,
                         album_artist = ?5,
                         artist_credits_json = ?6,
                         edition_visibility = ?7,
                         technical_visibility = ?8,
                         path_components_json = ?9,
                         primary_artwork_filename = ?10,
                         compatibility_verified = ?11,
                         compatibility_warnings_json = ?12,
                         rendered_at_unix_seconds = ?13
                     WHERE id = ?1",
                    params![
                        snapshot.id.as_uuid().to_string(),
                        snapshot.release_instance_id.as_uuid().to_string(),
                        &snapshot.export_profile,
                        &snapshot.album_title,
                        &snapshot.album_artist,
                        serde_json::to_string(&snapshot.artist_credits).map_err(|error| {
                            RepositoryError {
                                kind: RepositoryErrorKind::Storage,
                                message: format!(
                                    "failed to encode exported artist credits: {error}"
                                ),
                            }
                        })?,
                        qualifier_visibility_to_sql(&snapshot.edition_visibility),
                        qualifier_visibility_to_sql(&snapshot.technical_visibility),
                        serde_json::to_string(&snapshot.path_components).map_err(|error| {
                            RepositoryError {
                                kind: RepositoryErrorKind::Storage,
                                message: format!(
                                    "failed to encode exported path components: {error}"
                                ),
                            }
                        })?,
                        &snapshot.primary_artwork_filename,
                        snapshot.compatibility.verified,
                        serde_json::to_string(&snapshot.compatibility.warnings).map_err(
                            |error| RepositoryError {
                                kind: RepositoryErrorKind::Storage,
                                message: format!(
                                    "failed to encode exported compatibility warnings: {error}"
                                ),
                            },
                        )?,
                        snapshot.rendered_at_unix_seconds,
                    ],
                )
                .map_err(to_storage_error)?;
            if changed == 0 {
                return Err(RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: format!(
                        "exported metadata snapshot {} was not found",
                        snapshot.id.as_uuid()
                    ),
                });
            }
            Ok(())
        })
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

fn map_artist(row: &rusqlite::Row<'_>) -> rusqlite::Result<Artist> {
    Ok(Artist {
        id: parse_uuid_id::<ArtistId>(row.get_ref(0)?, 0)?,
        name: row.get(1)?,
        sort_name: row.get(2)?,
        musicbrainz_artist_id: parse_optional_mb_artist(row.get(3)?),
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
        import_batch_id: parse_uuid_id::<ImportBatchId>(row.get_ref(1)?, 1)?,
        source_id: parse_uuid_id::<SourceId>(row.get_ref(2)?, 2)?,
        release_id: row
            .get::<_, Option<String>>(3)?
            .map(|value| {
                ReleaseId::parse_str(&value).map_err(|error| invalid_column(3, error.to_string()))
            })
            .transpose()?,
        state: parse_release_instance_state(row.get::<_, String>(4)?),
        technical_variant: TechnicalVariant {
            format_family: parse_format_family(row.get::<_, String>(5)?),
            bitrate_mode: parse_bitrate_mode(row.get::<_, String>(6)?),
            bitrate_kbps: row.get::<_, Option<i64>>(7)?.map(|value| value as u32),
            sample_rate_hz: row.get::<_, Option<i64>>(8)?.map(|value| value as u32),
            bit_depth: row.get::<_, Option<i64>>(9)?.map(|value| value as u8),
            track_count: row.get::<_, i64>(10)? as u16,
            total_duration_seconds: row.get::<_, i64>(11)? as u32,
        },
        provenance: ProvenanceSnapshot {
            ingest_origin: parse_ingest_origin(row.get::<_, String>(12)?),
            original_source_path: row.get(13)?,
            imported_at_unix_seconds: row.get(14)?,
            gazelle_reference: row
                .get::<_, Option<String>>(15)?
                .map(|tracker| GazelleReference {
                    tracker,
                    torrent_id: row.get(16).unwrap_or(None),
                    release_group_id: row.get(17).unwrap_or(None),
                }),
        },
    })
}

fn map_track_instance(row: &rusqlite::Row<'_>) -> rusqlite::Result<TrackInstance> {
    Ok(TrackInstance {
        id: parse_uuid_id::<TrackInstanceId>(row.get_ref(0)?, 0)?,
        release_instance_id: parse_uuid_id::<ReleaseInstanceId>(row.get_ref(1)?, 1)?,
        track_id: parse_uuid_id::<TrackId>(row.get_ref(2)?, 2)?,
        observed_position: TrackPosition {
            disc_number: row.get::<_, i64>(3)? as u16,
            track_number: row.get::<_, i64>(4)? as u16,
        },
        observed_title: row.get(5)?,
        audio_properties: AudioProperties {
            format_family: parse_format_family(row.get::<_, String>(6)?),
            duration_ms: row.get::<_, Option<i64>>(7)?.map(|value| value as u32),
            bitrate_kbps: row.get::<_, Option<i64>>(8)?.map(|value| value as u32),
            sample_rate_hz: row.get::<_, Option<i64>>(9)?.map(|value| value as u32),
            bit_depth: row.get::<_, Option<i64>>(10)?.map(|value| value as u8),
        },
    })
}

fn map_track(row: &rusqlite::Row<'_>) -> rusqlite::Result<Track> {
    Ok(Track {
        id: parse_uuid_id::<TrackId>(row.get_ref(0)?, 0)?,
        release_id: parse_uuid_id::<ReleaseId>(row.get_ref(1)?, 1)?,
        position: TrackPosition {
            disc_number: row.get::<_, i64>(2)? as u16,
            track_number: row.get::<_, i64>(3)? as u16,
        },
        title: row.get(4)?,
        musicbrainz_track_id: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .map(crate::support::ids::MusicBrainzTrackId::parse_str)
            .transpose()
            .map_err(|error| invalid_column(5, error.to_string()))?,
        duration_ms: row.get::<_, Option<i64>>(6)?.map(|value| value as u32),
    })
}

fn map_file_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    Ok(FileRecord {
        id: parse_uuid_id::<FileId>(row.get_ref(0)?, 0)?,
        track_instance_id: parse_uuid_id::<TrackInstanceId>(row.get_ref(1)?, 1)?,
        role: parse_file_role(row.get::<_, String>(2)?),
        format_family: parse_format_family(row.get::<_, String>(3)?),
        path: PathBuf::from(row.get::<_, String>(4)?),
        checksum: row.get(5)?,
        size_bytes: row.get::<_, i64>(6)? as u64,
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

fn map_staging_manifest(row: &rusqlite::Row<'_>) -> rusqlite::Result<StagingManifest> {
    Ok(StagingManifest {
        id: parse_uuid_id::<StagingManifestId>(row.get_ref(0)?, 0)?,
        batch_id: parse_uuid_id::<ImportBatchId>(row.get_ref(1)?, 1)?,
        source: StagingManifestSource {
            kind: parse_source_kind(row.get(2)?),
            source_path: PathBuf::from(row.get::<_, String>(3)?),
        },
        discovered_files: parse_staged_files(row.get(4)?)
            .map_err(|error| invalid_column(4, error))?,
        auxiliary_files: parse_auxiliary_files(row.get(5)?)
            .map_err(|error| invalid_column(5, error))?,
        grouping: GroupingDecision {
            strategy: parse_grouping_strategy(row.get(6)?),
            groups: parse_staged_release_groups(row.get(7)?)
                .map_err(|error| invalid_column(7, error))?,
            notes: parse_string_list(row.get(8)?).map_err(|error| invalid_column(8, error))?,
        },
        captured_at_unix_seconds: row.get(9)?,
    })
}

fn map_ingest_evidence_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<IngestEvidenceRecord> {
    Ok(IngestEvidenceRecord {
        id: parse_uuid_id::<IngestEvidenceId>(row.get_ref(0)?, 0)?,
        batch_id: parse_uuid_id::<ImportBatchId>(row.get_ref(1)?, 1)?,
        subject: parse_ingest_evidence_subject(row.get(2)?, row.get(3)?)
            .map_err(|error| invalid_column(3, error))?,
        source: parse_ingest_evidence_source(row.get(4)?),
        observations: parse_observed_values(row.get(5)?)
            .map_err(|error| invalid_column(5, error))?,
        structured_payload: row.get(6)?,
        captured_at_unix_seconds: row.get(7)?,
    })
}

fn map_metadata_snapshot(row: &rusqlite::Row<'_>) -> rusqlite::Result<MetadataSnapshot> {
    Ok(MetadataSnapshot {
        id: parse_uuid_id::<MetadataSnapshotId>(row.get_ref(0)?, 0)?,
        subject: parse_metadata_subject(row.get(1)?, row.get(2)?)
            .map_err(|error| invalid_column(2, error))?,
        source: parse_metadata_snapshot_source(row.get(3)?),
        format: parse_snapshot_format(row.get(4)?),
        payload: row.get(5)?,
        captured_at_unix_seconds: row.get(6)?,
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

fn write_release_instance(
    transaction: &Transaction<'_>,
    release_instance: &ReleaseInstance,
) -> Result<(), RepositoryError> {
    transaction
        .execute(
            "INSERT INTO release_instances
             (id, import_batch_id, source_id, release_id, state, format_family, bitrate_mode,
              bitrate_kbps, sample_rate_hz, bit_depth, track_count, total_duration_seconds,
              ingest_origin, import_mode, duplicate_status, export_visibility_policy,
              original_source_path, imported_at_unix_seconds, gazelle_tracker,
              gazelle_torrent_id, gazelle_release_group_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, NULL, NULL,
                     NULL, ?14, ?15, ?16, ?17, ?18)",
            params![
                release_instance.id.as_uuid().to_string(),
                release_instance.import_batch_id.as_uuid().to_string(),
                release_instance.source_id.as_uuid().to_string(),
                release_instance
                    .release_id
                    .as_ref()
                    .map(|value| value.as_uuid().to_string()),
                release_instance_state_to_sql(&release_instance.state),
                format_family_to_sql(&release_instance.technical_variant.format_family),
                bitrate_mode_to_sql(&release_instance.technical_variant.bitrate_mode),
                release_instance
                    .technical_variant
                    .bitrate_kbps
                    .map(i64::from),
                release_instance
                    .technical_variant
                    .sample_rate_hz
                    .map(i64::from),
                release_instance.technical_variant.bit_depth.map(i64::from),
                i64::from(release_instance.technical_variant.track_count),
                i64::from(release_instance.technical_variant.total_duration_seconds),
                ingest_origin_to_sql(&release_instance.provenance.ingest_origin),
                &release_instance.provenance.original_source_path,
                release_instance.provenance.imported_at_unix_seconds,
                release_instance
                    .provenance
                    .gazelle_reference
                    .as_ref()
                    .map(|value| value.tracker.clone()),
                release_instance
                    .provenance
                    .gazelle_reference
                    .as_ref()
                    .and_then(|value| value.torrent_id.clone()),
                release_instance
                    .provenance
                    .gazelle_reference
                    .as_ref()
                    .and_then(|value| value.release_group_id.clone()),
            ],
        )
        .map_err(to_storage_error)?;
    Ok(())
}

fn write_track_instance(
    transaction: &Transaction<'_>,
    track_instance: &TrackInstance,
) -> Result<(), RepositoryError> {
    transaction
        .execute(
            "INSERT INTO track_instances
             (id, release_instance_id, track_id, observed_disc_number,
              observed_track_number, observed_title, format_family, duration_ms,
              bitrate_kbps, sample_rate_hz, bit_depth)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                track_instance.id.as_uuid().to_string(),
                track_instance.release_instance_id.as_uuid().to_string(),
                track_instance.track_id.as_uuid().to_string(),
                i64::from(track_instance.observed_position.disc_number),
                i64::from(track_instance.observed_position.track_number),
                &track_instance.observed_title,
                format_family_to_sql(&track_instance.audio_properties.format_family),
                track_instance.audio_properties.duration_ms.map(i64::from),
                track_instance.audio_properties.bitrate_kbps.map(i64::from),
                track_instance
                    .audio_properties
                    .sample_rate_hz
                    .map(i64::from),
                track_instance.audio_properties.bit_depth.map(i64::from),
            ],
        )
        .map_err(to_storage_error)?;
    Ok(())
}

fn write_file_record(
    transaction: &Transaction<'_>,
    file: &FileRecord,
) -> Result<(), RepositoryError> {
    transaction
        .execute(
            "INSERT INTO files
             (id, track_instance_id, role, format_family, path, checksum, size_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                file.id.as_uuid().to_string(),
                file.track_instance_id.as_uuid().to_string(),
                file_role_to_sql(&file.role),
                format_family_to_sql(&file.format_family),
                file.path.to_string_lossy().to_string(),
                &file.checksum,
                file.size_bytes as i64,
            ],
        )
        .map_err(to_storage_error)?;
    Ok(())
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
    FileId,
    ImportBatchId,
    IngestEvidenceId,
    IssueId,
    JobId,
    ManualOverrideId,
    MetadataSnapshotId,
    ReleaseGroupId,
    ReleaseId,
    ReleaseInstanceId,
    SourceId,
    StagingManifestId,
    TrackId,
    TrackInstanceId,
    MusicBrainzReleaseGroupId,
    MusicBrainzReleaseId
);

fn parse_optional_mb_release_group(raw: Option<String>) -> Option<MusicBrainzReleaseGroupId> {
    raw.and_then(|value| MusicBrainzReleaseGroupId::parse_str(&value).ok())
}

fn parse_optional_mb_artist(
    raw: Option<String>,
) -> Option<crate::support::ids::MusicBrainzArtistId> {
    raw.and_then(|value| crate::support::ids::MusicBrainzArtistId::parse_str(&value).ok())
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

fn release_group_kind_to_sql(value: &ReleaseGroupKind) -> &str {
    match value {
        ReleaseGroupKind::Album => "album",
        ReleaseGroupKind::Ep => "ep",
        ReleaseGroupKind::Single => "single",
        ReleaseGroupKind::Live => "live",
        ReleaseGroupKind::Compilation => "compilation",
        ReleaseGroupKind::Soundtrack => "soundtrack",
        ReleaseGroupKind::Other(_) => "other",
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

fn parse_file_role(value: String) -> FileRole {
    match value.as_str() {
        "source" => FileRole::Source,
        _ => FileRole::Managed,
    }
}

fn file_role_to_sql(value: &FileRole) -> String {
    match value {
        FileRole::Source => "source",
        FileRole::Managed => "managed",
    }
    .to_string()
}

fn bitrate_mode_to_sql(value: &BitrateMode) -> String {
    match value {
        BitrateMode::Constant => "constant",
        BitrateMode::Variable => "variable",
        BitrateMode::Lossless => "lossless",
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

fn ingest_origin_to_sql(value: &IngestOrigin) -> String {
    match value {
        IngestOrigin::WatchDirectory => "watch_directory",
        IngestOrigin::ApiPush => "api_push",
        IngestOrigin::ManualAdd => "manual_add",
    }
    .to_string()
}

fn parse_candidate_provider(value: String) -> CandidateProvider {
    match value.as_str() {
        "musicbrainz" => CandidateProvider::MusicBrainz,
        _ => CandidateProvider::Discogs,
    }
}

fn candidate_provider_to_sql(value: &CandidateProvider) -> String {
    match value {
        CandidateProvider::MusicBrainz => "musicbrainz",
        CandidateProvider::Discogs => "discogs",
    }
    .to_string()
}

fn parse_candidate_subject(kind: String, provider_id: String) -> CandidateSubject {
    match kind.as_str() {
        "release" => CandidateSubject::Release { provider_id },
        _ => CandidateSubject::ReleaseGroup { provider_id },
    }
}

fn candidate_subject_kind_to_sql(value: &CandidateSubject) -> String {
    match value {
        CandidateSubject::Release { .. } => "release",
        CandidateSubject::ReleaseGroup { .. } => "release_group",
    }
    .to_string()
}

fn candidate_subject_id(value: &CandidateSubject) -> String {
    match value {
        CandidateSubject::Release { provider_id }
        | CandidateSubject::ReleaseGroup { provider_id } => provider_id.clone(),
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

fn evidence_kind_to_sql(value: &EvidenceKind) -> String {
    match value {
        EvidenceKind::ArtistMatch => "artist_match",
        EvidenceKind::AlbumTitleMatch => "album_title_match",
        EvidenceKind::TrackCountMatch => "track_count_match",
        EvidenceKind::DurationAlignment => "duration_alignment",
        EvidenceKind::DiscCountMatch => "disc_count_match",
        EvidenceKind::DateProximity => "date_proximity",
        EvidenceKind::LabelCatalogAlignment => "label_catalog_alignment",
        EvidenceKind::FilenameSimilarity => "filename_similarity",
        EvidenceKind::GazelleConsistency => "gazelle_consistency",
        EvidenceKind::Other(other) => other,
    }
    .to_string()
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

fn parse_grouping_strategy(value: String) -> GroupingStrategy {
    match value.as_str() {
        "common_parent_directory" => GroupingStrategy::CommonParentDirectory,
        "shared_album_metadata" => GroupingStrategy::SharedAlbumMetadata,
        "track_number_continuity" => GroupingStrategy::TrackNumberContinuity,
        _ => GroupingStrategy::ManualManifest,
    }
}

fn grouping_strategy_to_sql(value: &GroupingStrategy) -> &'static str {
    match value {
        GroupingStrategy::CommonParentDirectory => "common_parent_directory",
        GroupingStrategy::SharedAlbumMetadata => "shared_album_metadata",
        GroupingStrategy::TrackNumberContinuity => "track_number_continuity",
        GroupingStrategy::ManualManifest => "manual_manifest",
    }
}

fn parse_auxiliary_file_role(value: &str, description: Option<String>) -> AuxiliaryFileRole {
    match value {
        "gazelle_yaml" => AuxiliaryFileRole::GazelleYaml,
        "artwork" => AuxiliaryFileRole::Artwork,
        "cue_sheet" => AuxiliaryFileRole::CueSheet,
        "log" => AuxiliaryFileRole::Log,
        _ => AuxiliaryFileRole::Other {
            description: description.unwrap_or_else(|| "other".to_string()),
        },
    }
}

fn auxiliary_file_role_to_sql(value: &AuxiliaryFileRole) -> (&'static str, Option<String>) {
    match value {
        AuxiliaryFileRole::GazelleYaml => ("gazelle_yaml", None),
        AuxiliaryFileRole::Artwork => ("artwork", None),
        AuxiliaryFileRole::CueSheet => ("cue_sheet", None),
        AuxiliaryFileRole::Log => ("log", None),
        AuxiliaryFileRole::Other { description } => ("other", Some(description.clone())),
    }
}

fn parse_file_fingerprint(value: &Value) -> Result<FileFingerprint, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "file fingerprint must be an object".to_string())?;
    let kind = get_json_string(object, "kind")?;
    let value = get_json_string(object, "value")?;
    match kind.as_str() {
        "content_hash" => Ok(FileFingerprint::ContentHash(value)),
        "lightweight" => Ok(FileFingerprint::LightweightFingerprint(value)),
        _ => Err(format!("unknown file fingerprint kind '{kind}'")),
    }
}

fn file_fingerprint_to_json(value: &FileFingerprint) -> Value {
    match value {
        FileFingerprint::ContentHash(hash) => {
            json!({ "kind": "content_hash", "value": hash })
        }
        FileFingerprint::LightweightFingerprint(fingerprint) => {
            json!({ "kind": "lightweight", "value": fingerprint })
        }
    }
}

fn observed_tag_to_json(value: &ObservedTag) -> Value {
    json!({
        "key": value.key,
        "value": value.value,
    })
}

fn parse_observed_tag(value: &Value) -> Result<ObservedTag, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "observed tag must be an object".to_string())?;
    Ok(ObservedTag {
        key: get_json_string(object, "key")?,
        value: get_json_string(object, "value")?,
    })
}

fn serialize_staged_files(values: &[StagedFile]) -> Result<String, RepositoryError> {
    serde_json::to_string(
        &values
            .iter()
            .map(|value| {
                json!({
                    "path": value.path.to_string_lossy(),
                    "fingerprint": file_fingerprint_to_json(&value.fingerprint),
                    "observed_tags": value
                        .observed_tags
                        .iter()
                        .map(observed_tag_to_json)
                        .collect::<Vec<_>>(),
                    "duration_ms": value.duration_ms,
                    "format_family": format_family_to_sql(&value.format_family),
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| storage_error(format!("failed to serialize staged files: {error}")))
}

fn serialize_evidence_notes(values: &[EvidenceNote]) -> Result<String, String> {
    serde_json::to_string(
        &values
            .iter()
            .map(|value| {
                json!({
                    "kind": evidence_kind_to_sql(&value.kind),
                    "detail": value.detail,
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| format!("failed to serialize evidence notes: {error}"))
}

fn serialize_provider_provenance(value: &ProviderProvenance) -> Result<String, String> {
    serde_json::to_string(&json!({
        "provider_name": value.provider_name,
        "query": value.query,
        "fetched_at_unix_seconds": value.fetched_at_unix_seconds,
    }))
    .map_err(|error| format!("failed to serialize provider provenance: {error}"))
}

fn parse_staged_files(raw: String) -> Result<Vec<StagedFile>, String> {
    let values: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    values
        .as_array()
        .ok_or_else(|| "staged files must be a JSON array".to_string())?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| "staged file must be an object".to_string())?;
            let observed_tags = object
                .get("observed_tags")
                .and_then(Value::as_array)
                .ok_or_else(|| "staged file observed_tags must be an array".to_string())?
                .iter()
                .map(parse_observed_tag)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(StagedFile {
                path: PathBuf::from(get_json_string(object, "path")?),
                fingerprint: parse_file_fingerprint(
                    object
                        .get("fingerprint")
                        .ok_or_else(|| "staged file fingerprint is required".to_string())?,
                )?,
                observed_tags,
                duration_ms: object
                    .get("duration_ms")
                    .and_then(Value::as_u64)
                    .map(|value| value as u32),
                format_family: parse_format_family(get_json_string(object, "format_family")?),
            })
        })
        .collect()
}

fn serialize_auxiliary_files(values: &[AuxiliaryFile]) -> Result<String, RepositoryError> {
    serde_json::to_string(
        &values
            .iter()
            .map(|value| {
                let (role, description) = auxiliary_file_role_to_sql(&value.role);
                json!({
                    "path": value.path.to_string_lossy(),
                    "role": role,
                    "description": description,
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| storage_error(format!("failed to serialize auxiliary files: {error}")))
}

fn parse_auxiliary_files(raw: String) -> Result<Vec<AuxiliaryFile>, String> {
    let values: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    values
        .as_array()
        .ok_or_else(|| "auxiliary files must be a JSON array".to_string())?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| "auxiliary file must be an object".to_string())?;
            Ok(AuxiliaryFile {
                path: PathBuf::from(get_json_string(object, "path")?),
                role: parse_auxiliary_file_role(
                    &get_json_string(object, "role")?,
                    object
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                ),
            })
        })
        .collect()
}

fn serialize_staged_release_groups(
    values: &[StagedReleaseGroup],
) -> Result<String, RepositoryError> {
    serde_json::to_string(
        &values
            .iter()
            .map(|value| {
                json!({
                    "key": value.key,
                    "file_paths": value
                        .file_paths
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect::<Vec<_>>(),
                    "auxiliary_paths": value
                        .auxiliary_paths
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| {
        storage_error(format!(
            "failed to serialize staged release groups: {error}"
        ))
    })
}

fn parse_staged_release_groups(raw: String) -> Result<Vec<StagedReleaseGroup>, String> {
    let values: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    values
        .as_array()
        .ok_or_else(|| "staged release groups must be a JSON array".to_string())?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| "staged release group must be an object".to_string())?;
            Ok(StagedReleaseGroup {
                key: get_json_string(object, "key")?,
                file_paths: parse_path_array(
                    object.get("file_paths").ok_or_else(|| {
                        "staged release group file_paths are required".to_string()
                    })?,
                )?,
                auxiliary_paths: parse_path_array(object.get("auxiliary_paths").ok_or_else(
                    || "staged release group auxiliary_paths are required".to_string(),
                )?)?,
            })
        })
        .collect()
}

fn parse_path_array(value: &Value) -> Result<Vec<PathBuf>, String> {
    value
        .as_array()
        .ok_or_else(|| "path list must be an array".to_string())?
        .iter()
        .map(|item| {
            item.as_str()
                .map(PathBuf::from)
                .ok_or_else(|| "path list entries must be strings".to_string())
        })
        .collect()
}

fn parse_ingest_evidence_source(value: String) -> IngestEvidenceSource {
    match value.as_str() {
        "embedded_tags" => IngestEvidenceSource::EmbeddedTags,
        "file_name" => IngestEvidenceSource::FileName,
        "directory_structure" => IngestEvidenceSource::DirectoryStructure,
        "gazelle_yaml" => IngestEvidenceSource::GazelleYaml,
        _ => IngestEvidenceSource::AuxiliaryFile,
    }
}

fn ingest_evidence_source_to_sql(value: &IngestEvidenceSource) -> &'static str {
    match value {
        IngestEvidenceSource::EmbeddedTags => "embedded_tags",
        IngestEvidenceSource::FileName => "file_name",
        IngestEvidenceSource::DirectoryStructure => "directory_structure",
        IngestEvidenceSource::GazelleYaml => "gazelle_yaml",
        IngestEvidenceSource::AuxiliaryFile => "auxiliary_file",
    }
}

fn parse_ingest_evidence_subject(
    kind: String,
    value: String,
) -> Result<IngestEvidenceSubject, String> {
    match kind.as_str() {
        "discovered_path" => Ok(IngestEvidenceSubject::DiscoveredPath(PathBuf::from(value))),
        "grouped_release_input" => {
            Ok(IngestEvidenceSubject::GroupedReleaseInput { group_key: value })
        }
        _ => Err(format!("unknown ingest evidence subject kind '{kind}'")),
    }
}

fn ingest_evidence_subject_to_sql(value: &IngestEvidenceSubject) -> (&'static str, String) {
    match value {
        IngestEvidenceSubject::DiscoveredPath(path) => {
            ("discovered_path", path.to_string_lossy().to_string())
        }
        IngestEvidenceSubject::GroupedReleaseInput { group_key } => {
            ("grouped_release_input", group_key.clone())
        }
    }
}

fn parse_observed_value_kind(value: &str) -> Result<ObservedValueKind, String> {
    match value {
        "artist" => Ok(ObservedValueKind::Artist),
        "release_title" => Ok(ObservedValueKind::ReleaseTitle),
        "release_year" => Ok(ObservedValueKind::ReleaseYear),
        "track_title" => Ok(ObservedValueKind::TrackTitle),
        "track_number" => Ok(ObservedValueKind::TrackNumber),
        "disc_number" => Ok(ObservedValueKind::DiscNumber),
        "duration_ms" => Ok(ObservedValueKind::DurationMs),
        "format_family" => Ok(ObservedValueKind::FormatFamily),
        "media_descriptor" => Ok(ObservedValueKind::MediaDescriptor),
        "source_descriptor" => Ok(ObservedValueKind::SourceDescriptor),
        "tracker_identifier" => Ok(ObservedValueKind::TrackerIdentifier),
        _ => Err(format!("unknown observed value kind '{value}'")),
    }
}

fn observed_value_kind_to_sql(value: &ObservedValueKind) -> &'static str {
    match value {
        ObservedValueKind::Artist => "artist",
        ObservedValueKind::ReleaseTitle => "release_title",
        ObservedValueKind::ReleaseYear => "release_year",
        ObservedValueKind::TrackTitle => "track_title",
        ObservedValueKind::TrackNumber => "track_number",
        ObservedValueKind::DiscNumber => "disc_number",
        ObservedValueKind::DurationMs => "duration_ms",
        ObservedValueKind::FormatFamily => "format_family",
        ObservedValueKind::Label => "label",
        ObservedValueKind::CatalogNumber => "catalog_number",
        ObservedValueKind::MediaDescriptor => "media_descriptor",
        ObservedValueKind::SourceDescriptor => "source_descriptor",
        ObservedValueKind::TrackerIdentifier => "tracker_identifier",
    }
}

fn serialize_observed_values(values: &[ObservedValue]) -> Result<String, RepositoryError> {
    serde_json::to_string(
        &values
            .iter()
            .map(|value| {
                json!({
                    "kind": observed_value_kind_to_sql(&value.kind),
                    "value": value.value,
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| storage_error(format!("failed to serialize observed values: {error}")))
}

fn parse_observed_values(raw: String) -> Result<Vec<ObservedValue>, String> {
    let values: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    values
        .as_array()
        .ok_or_else(|| "observed values must be a JSON array".to_string())?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| "observed value must be an object".to_string())?;
            Ok(ObservedValue {
                kind: parse_observed_value_kind(&get_json_string(object, "kind")?)?,
                value: get_json_string(object, "value")?,
            })
        })
        .collect()
}

fn parse_metadata_snapshot_source(value: String) -> MetadataSnapshotSource {
    match value.as_str() {
        "embedded_tags" => MetadataSnapshotSource::EmbeddedTags,
        "filename_heuristics" => MetadataSnapshotSource::FileNameHeuristics,
        "directory_structure" => MetadataSnapshotSource::DirectoryStructure,
        "gazelle_yaml" => MetadataSnapshotSource::GazelleYaml,
        "musicbrainz_payload" => MetadataSnapshotSource::MusicBrainzPayload,
        _ => MetadataSnapshotSource::DiscogsPayload,
    }
}

fn metadata_snapshot_source_to_sql(value: &MetadataSnapshotSource) -> &'static str {
    match value {
        MetadataSnapshotSource::EmbeddedTags => "embedded_tags",
        MetadataSnapshotSource::FileNameHeuristics => "filename_heuristics",
        MetadataSnapshotSource::DirectoryStructure => "directory_structure",
        MetadataSnapshotSource::GazelleYaml => "gazelle_yaml",
        MetadataSnapshotSource::MusicBrainzPayload => "musicbrainz_payload",
        MetadataSnapshotSource::DiscogsPayload => "discogs_payload",
    }
}

fn parse_snapshot_format(value: String) -> SnapshotFormat {
    match value.as_str() {
        "json" => SnapshotFormat::Json,
        "yaml" => SnapshotFormat::Yaml,
        _ => SnapshotFormat::Text,
    }
}

fn snapshot_format_to_sql(value: &SnapshotFormat) -> &'static str {
    match value {
        SnapshotFormat::Json => "json",
        SnapshotFormat::Yaml => "yaml",
        SnapshotFormat::Text => "text",
    }
}

fn parse_metadata_subject(kind: String, value: String) -> Result<MetadataSubject, String> {
    match kind.as_str() {
        "import_batch" => Ok(MetadataSubject::ImportBatch(
            ImportBatchId::parse_str(&value).map_err(|error| error.to_string())?,
        )),
        "release_instance" => Ok(MetadataSubject::ReleaseInstance(
            ReleaseInstanceId::parse_str(&value).map_err(|error| error.to_string())?,
        )),
        "file" => Ok(MetadataSubject::File(
            FileId::parse_str(&value).map_err(|error| error.to_string())?,
        )),
        _ => Err(format!("unknown metadata subject kind '{kind}'")),
    }
}

fn metadata_subject_to_sql(value: &MetadataSubject) -> (&'static str, String) {
    match value {
        MetadataSubject::ImportBatch(id) => ("import_batch", id.as_uuid().to_string()),
        MetadataSubject::ReleaseInstance(id) => ("release_instance", id.as_uuid().to_string()),
        MetadataSubject::File(id) => ("file", id.as_uuid().to_string()),
    }
}

fn serialize_string_array(values: &[String]) -> Result<String, RepositoryError> {
    serde_json::to_string(values)
        .map_err(|error| storage_error(format!("failed to serialize string array: {error}")))
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

fn map_manual_override(row: &rusqlite::Row<'_>) -> rusqlite::Result<ManualOverride> {
    Ok(ManualOverride {
        id: parse_uuid_id::<ManualOverrideId>(row.get_ref(0)?, 0)?,
        subject: parse_manual_override_subject(row.get(1)?, row.get(2)?)?,
        field: parse_manual_override_field(row.get(3)?),
        value: row.get(4)?,
        note: row.get(5)?,
        created_by: row.get(6)?,
        created_at_unix_seconds: row.get(7)?,
    })
}

fn parse_manual_override_subject(
    kind: String,
    id: String,
) -> Result<OverrideSubject, rusqlite::Error> {
    match kind.as_str() {
        "release" => Ok(OverrideSubject::Release(
            ReleaseId::parse_str(&id).map_err(|error| invalid_column(2, error.to_string()))?,
        )),
        "release_instance" => Ok(OverrideSubject::ReleaseInstance(
            ReleaseInstanceId::parse_str(&id)
                .map_err(|error| invalid_column(2, error.to_string()))?,
        )),
        "track" => Ok(OverrideSubject::Track(
            TrackId::parse_str(&id).map_err(|error| invalid_column(2, error.to_string()))?,
        )),
        other => Err(invalid_column(
            1,
            format!("unknown manual override subject kind '{other}'"),
        )),
    }
}

fn manual_override_subject_kind_to_sql(subject: &OverrideSubject) -> &'static str {
    match subject {
        OverrideSubject::Release(_) => "release",
        OverrideSubject::ReleaseInstance(_) => "release_instance",
        OverrideSubject::Track(_) => "track",
    }
}

fn manual_override_subject_id_to_sql(subject: &OverrideSubject) -> String {
    match subject {
        OverrideSubject::Release(id) => id.as_uuid().to_string(),
        OverrideSubject::ReleaseInstance(id) => id.as_uuid().to_string(),
        OverrideSubject::Track(id) => id.as_uuid().to_string(),
    }
}

fn parse_manual_override_field(value: String) -> OverrideField {
    match value.as_str() {
        "release_match" => OverrideField::ReleaseMatch,
        "title" => OverrideField::Title,
        "album_artist" => OverrideField::AlbumArtist,
        "artist_credit" => OverrideField::ArtistCredit,
        "track_title" => OverrideField::TrackTitle,
        "release_date" => OverrideField::ReleaseDate,
        "edition_qualifier" => OverrideField::EditionQualifier,
        _ => OverrideField::ArtworkSelection,
    }
}

fn manual_override_field_to_sql(field: &OverrideField) -> &'static str {
    match field {
        OverrideField::ReleaseMatch => "release_match",
        OverrideField::Title => "title",
        OverrideField::AlbumArtist => "album_artist",
        OverrideField::ArtistCredit => "artist_credit",
        OverrideField::TrackTitle => "track_title",
        OverrideField::ReleaseDate => "release_date",
        OverrideField::EditionQualifier => "edition_qualifier",
        OverrideField::ArtworkSelection => "artwork_selection",
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

fn qualifier_visibility_to_sql(value: &QualifierVisibility) -> &'static str {
    match value {
        QualifierVisibility::Hidden => "hidden",
        QualifierVisibility::PathOnly => "path_only",
        QualifierVisibility::TagsAndPath => "tags_and_path",
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
    use crate::application::config::ValidatedRuntimeConfig;
    use crate::application::ingest::WatchDiscoveryService;
    use crate::application::repository::{
        ExportRepository, IngestEvidenceRepository, IssueRepository, JobCommandRepository,
        JobRepository, ManualOverrideCommandRepository, ManualOverrideListQuery,
        ManualOverrideRepository, MetadataSnapshotRepository, ReleaseCommandRepository,
        ReleaseInstanceCommandRepository, ReleaseInstanceRepository, ReleaseRepository,
        StagingManifestRepository,
    };
    use crate::domain::artist::Artist;
    use crate::domain::candidate_match::{
        CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
        EvidenceNote, ProviderProvenance,
    };
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::issue::{IssueState, IssueSubject, IssueType};
    use crate::domain::job::{JobStatus, JobSubject, JobTrigger, JobType};
    use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
    use crate::domain::metadata_snapshot::{MetadataSnapshotSource, SnapshotFormat};
    use crate::domain::release::{PartialDate, Release, ReleaseEdition};
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        ReleaseInstanceState, TechnicalVariant,
    };
    use crate::domain::track_instance::{AudioProperties, TrackInstance};
    use id3::TagLike;
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
    fn repositories_list_canonical_tracks_for_release() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);

        let tracks = repositories
            .list_tracks_for_release(&parse_uuid(SeedIds::RELEASE))
            .expect("track query should succeed");
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].position.disc_number, 1);
        assert_eq!(tracks[0].position.track_number, 1);
        assert_eq!(tracks[0].title, "15 Step");
        assert_eq!(tracks[1].position.track_number, 2);
        assert_eq!(tracks[1].title, "Bodysnatchers");
    }

    #[test]
    fn repositories_replace_track_instances_and_managed_files() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let release_instance_id: ReleaseInstanceId = parse_uuid(SeedIds::RELEASE_INSTANCE);
        let track_one_id: TrackId = parse_uuid(SeedIds::TRACK_ONE);
        let track_instance = TrackInstance {
            id: TrackInstanceId::new(),
            release_instance_id: release_instance_id.clone(),
            track_id: track_one_id,
            observed_position: TrackPosition {
                disc_number: 1,
                track_number: 1,
            },
            observed_title: Some("15 Step".to_string()),
            audio_properties: AudioProperties {
                format_family: FormatFamily::Flac,
                duration_ms: Some(237_000),
                bitrate_kbps: None,
                sample_rate_hz: Some(44_100),
                bit_depth: Some(16),
            },
        };
        let source_file = FileRecord {
            id: FileId::new(),
            track_instance_id: track_instance.id.clone(),
            role: FileRole::Source,
            format_family: FormatFamily::Flac,
            path: PathBuf::from("/incoming/radiohead/In Rainbows/01 - 15 Step.flac"),
            checksum: None,
            size_bytes: 1024,
        };
        let managed_file = FileRecord {
            id: FileId::new(),
            track_instance_id: track_instance.id.clone(),
            role: FileRole::Managed,
            format_family: FormatFamily::Flac,
            path: PathBuf::from(
                "/library/Radiohead/In Rainbows/2007 - 2007 CD/FLAC-lossless-na-44100-16/Incoming/01 - 15 Step.flac",
            ),
            checksum: None,
            size_bytes: 1024,
        };

        repositories
            .replace_track_instances_and_files(
                &release_instance_id,
                std::slice::from_ref(&track_instance),
                &[source_file.clone(), managed_file.clone()],
            )
            .expect("track instances and files should persist");

        let stored_track_instances = repositories
            .list_track_instances_for_release_instance(&release_instance_id)
            .expect("track instances should load");
        assert_eq!(stored_track_instances, vec![track_instance]);

        let stored_managed_files = repositories
            .list_files_for_release_instance(&release_instance_id, Some(FileRole::Managed))
            .expect("managed files should load");
        assert_eq!(stored_managed_files, vec![managed_file]);
    }

    #[test]
    fn repositories_persist_provisional_release_instances_and_candidates() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let import_batch_id: ImportBatchId = parse_uuid(SeedIds::IMPORT_BATCH);
        let source_id: SourceId = parse_uuid(SeedIds::SOURCE);
        let release_instance = ReleaseInstance {
            id: ReleaseInstanceId::new(),
            import_batch_id: import_batch_id.clone(),
            source_id,
            release_id: None,
            state: ReleaseInstanceState::NeedsReview,
            technical_variant: TechnicalVariant {
                format_family: FormatFamily::Mp3,
                bitrate_mode: BitrateMode::Variable,
                bitrate_kbps: None,
                sample_rate_hz: None,
                bit_depth: None,
                track_count: 1,
                total_duration_seconds: 245,
            },
            provenance: ProvenanceSnapshot {
                ingest_origin: IngestOrigin::ManualAdd,
                original_source_path: "/incoming/Kid A/01 Everything.mp3".to_string(),
                imported_at_unix_seconds: 321,
                gazelle_reference: None,
            },
        };

        repositories
            .create_release_instance(&release_instance)
            .expect("release instance should persist");
        repositories
            .replace_candidate_matches(
                &release_instance.id,
                &[CandidateMatch {
                    id: CandidateMatchId::new(),
                    release_instance_id: release_instance.id.clone(),
                    provider: CandidateProvider::MusicBrainz,
                    subject: CandidateSubject::Release {
                        provider_id: "mb-release-1".to_string(),
                    },
                    normalized_score: CandidateScore::new(0.88),
                    evidence_matches: vec![EvidenceNote {
                        kind: EvidenceKind::ArtistMatch,
                        detail: "artist names aligned".to_string(),
                    }],
                    mismatches: Vec::new(),
                    unresolved_ambiguities: vec!["vinyl reissue also matched".to_string()],
                    provider_provenance: ProviderProvenance {
                        provider_name: "musicbrainz".to_string(),
                        query: "\"Kid A\" AND artist:\"Radiohead\"".to_string(),
                        fetched_at_unix_seconds: 322,
                    },
                }],
            )
            .expect("candidate matches should persist");

        let stored_instances = repositories
            .list_release_instances_for_batch(&import_batch_id)
            .expect("batch release instances should load");
        assert!(
            stored_instances
                .iter()
                .any(|item| item.id == release_instance.id && item.release_id.is_none())
        );

        let stored_candidates = repositories
            .list_candidate_matches(&release_instance.id, &PageRequest::new(10, 0))
            .expect("candidate matches should load");
        assert_eq!(stored_candidates.total, 1);
        assert_eq!(stored_candidates.items[0].normalized_score.value(), 0.88);
    }

    #[test]
    fn repositories_create_and_lookup_canonical_release_rows() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let artist = Artist {
            id: ArtistId::new(),
            name: "Boards of Canada".to_string(),
            sort_name: Some("Boards of Canada".to_string()),
            musicbrainz_artist_id: crate::support::ids::MusicBrainzArtistId::parse_str(
                "11111111-1111-4111-8111-111111111111",
            )
            .ok(),
        };
        repositories
            .create_artist(&artist)
            .expect("artist should persist");

        let release_group = ReleaseGroup {
            id: ReleaseGroupId::new(),
            primary_artist_id: artist.id.clone(),
            title: "Music Has the Right to Children".to_string(),
            kind: ReleaseGroupKind::Album,
            musicbrainz_release_group_id:
                crate::support::ids::MusicBrainzReleaseGroupId::parse_str(
                    "22222222-2222-4222-8222-222222222222",
                )
                .ok(),
        };
        repositories
            .create_release_group(&release_group)
            .expect("release group should persist");

        let release = Release {
            id: ReleaseId::new(),
            release_group_id: release_group.id.clone(),
            primary_artist_id: artist.id.clone(),
            title: "Music Has the Right to Children".to_string(),
            musicbrainz_release_id: crate::support::ids::MusicBrainzReleaseId::parse_str(
                "33333333-3333-4333-8333-333333333333",
            )
            .ok(),
            discogs_release_id: None,
            edition: ReleaseEdition {
                edition_title: Some("Warp CD".to_string()),
                disambiguation: None,
                country: Some("GB".to_string()),
                label: Some("Warp".to_string()),
                catalog_number: Some("WARPCD55".to_string()),
                release_date: Some(PartialDate {
                    year: 1998,
                    month: Some(4),
                    day: Some(20),
                }),
            },
        };
        repositories
            .create_release(&release)
            .expect("release should persist");

        let stored_artist = repositories
            .find_artist_by_musicbrainz_id("11111111-1111-4111-8111-111111111111")
            .expect("artist lookup should succeed")
            .expect("artist should exist");
        assert_eq!(stored_artist.name, "Boards of Canada");

        let stored_group = repositories
            .find_release_group_by_musicbrainz_id("22222222-2222-4222-8222-222222222222")
            .expect("release group lookup should succeed")
            .expect("release group should exist");
        assert_eq!(stored_group.title, "Music Has the Right to Children");

        let stored_release = repositories
            .find_release_by_musicbrainz_id("33333333-3333-4333-8333-333333333333")
            .expect("release lookup should succeed")
            .expect("release should exist");
        assert_eq!(
            stored_release.edition.catalog_number.as_deref(),
            Some("WARPCD55")
        );
    }

    #[test]
    fn repositories_persist_and_filter_manual_release_overrides() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let override_record = ManualOverride {
            id: ManualOverrideId::new(),
            subject: OverrideSubject::ReleaseInstance(parse_uuid(SeedIds::RELEASE_INSTANCE)),
            field: OverrideField::ReleaseMatch,
            value: SeedIds::RELEASE.to_string(),
            note: Some("chosen by operator".to_string()),
            created_by: "operator".to_string(),
            created_at_unix_seconds: 500,
        };
        repositories
            .create_manual_override(&override_record)
            .expect("manual override should persist");

        let stored = repositories
            .get_manual_override(&override_record.id)
            .expect("lookup should succeed")
            .expect("override should exist");
        assert_eq!(stored.field, OverrideField::ReleaseMatch);

        let listed = repositories
            .list_manual_overrides(&ManualOverrideListQuery {
                subject: Some(OverrideSubject::ReleaseInstance(parse_uuid(
                    SeedIds::RELEASE_INSTANCE,
                ))),
                field: Some(OverrideField::ReleaseMatch),
                page: PageRequest::new(10, 0),
            })
            .expect("query should succeed");
        assert_eq!(listed.total, 1);
        assert_eq!(listed.items[0].value, SeedIds::RELEASE);
    }

    #[test]
    fn repositories_persist_exported_metadata_snapshots() {
        let (context, _path) = seeded_context();
        let repositories = SqliteRepositories::new(context);
        let mut snapshot = ExportedMetadataSnapshot {
            id: ExportedMetadataSnapshotId::new(),
            release_instance_id: parse_uuid(SeedIds::RELEASE_INSTANCE),
            export_profile: "generic_player".to_string(),
            album_title: "In Rainbows [2007 CD]".to_string(),
            album_artist: "Radiohead".to_string(),
            artist_credits: vec!["Radiohead".to_string()],
            edition_visibility: QualifierVisibility::TagsAndPath,
            technical_visibility: QualifierVisibility::PathOnly,
            path_components: vec![
                "Radiohead".to_string(),
                "In Rainbows [2007 CD] [FLAC lossless]".to_string(),
            ],
            primary_artwork_filename: Some("cover.jpg".to_string()),
            compatibility: CompatibilityReport {
                verified: true,
                warnings: vec!["none".to_string()],
            },
            rendered_at_unix_seconds: 501,
        };
        repositories
            .create_exported_metadata_snapshot(&snapshot)
            .expect("snapshot should persist");

        let stored = repositories
            .get_exported_metadata(&snapshot.id)
            .expect("lookup should succeed")
            .expect("snapshot should exist");
        assert_eq!(stored.album_title, "In Rainbows [2007 CD]");
        assert_eq!(
            stored.primary_artwork_filename,
            Some("cover.jpg".to_string())
        );

        snapshot.compatibility.verified = false;
        snapshot.compatibility.warnings = vec!["player-visible collision detected".to_string()];
        repositories
            .update_exported_metadata_snapshot(&snapshot)
            .expect("snapshot update should persist");

        let updated = repositories
            .get_exported_metadata(&snapshot.id)
            .expect("lookup should succeed")
            .expect("snapshot should exist");
        assert!(!updated.compatibility.verified);
        assert_eq!(
            updated.compatibility.warnings,
            vec!["player-visible collision detected".to_string()]
        );
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

        let release_instance_issues = repositories
            .list_issues(&IssueListQuery {
                issue_type: Some(IssueType::DuplicateReleaseInstance),
                subject: Some(IssueSubject::ReleaseInstance(parse_uuid(
                    SeedIds::RELEASE_INSTANCE,
                ))),
                ..IssueListQuery::default()
            })
            .expect("query should succeed");
        assert_eq!(release_instance_issues.total, 1);
        assert_eq!(
            release_instance_issues.items[0].summary,
            "Duplicate import detected"
        );

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

    #[test]
    fn repositories_persist_batch_analysis_output_with_sqlite() {
        let database_path =
            std::env::temp_dir().join(format!("discern-analyze-test-{}.db", Uuid::new_v4()));
        let context = SqliteRepositoryContext::open(&database_path).expect("context should open");
        context
            .with_write_transaction(|transaction| {
                apply_migrations(transaction)?;
                Ok(())
            })
            .expect("migrations should apply");
        let repositories = SqliteRepositories::new(context.clone());

        let temp_root =
            std::env::temp_dir().join(format!("discern-analyze-fixture-{}", Uuid::new_v4()));
        let album_path = temp_root.join("Kid A");
        std::fs::create_dir_all(&album_path).expect("album directory should be created");
        let mp3_path = album_path.join("01 Everything.mp3");
        std::fs::write(&mp3_path, b"").expect("mp3 placeholder should exist");

        let mut tag = id3::Tag::new();
        tag.set_artist("Radiohead");
        tag.set_album("Kid A");
        tag.set_title("Everything in Its Right Place");
        tag.write_to_path(&mp3_path, id3::Version::Id3v24)
            .expect("id3 tag should be written");

        let yaml_path = album_path.join("release.yaml");
        std::fs::write(
            &yaml_path,
            "release_name: Kid A\nartist: Radiohead\nyear: 2000\n",
        )
        .expect("yaml should be written");

        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = WatchDiscoveryService::new(repositories.clone(), config);
        let submission = service
            .submit_manual_path("chris", album_path.clone(), 500)
            .expect("manual intake should succeed");

        let report = service
            .analyze_import_batch(&submission.batch.id, 501)
            .expect("batch analysis should succeed");

        let manifests = repositories
            .list_staging_manifests_for_batch(&submission.batch.id)
            .expect("manifest query should succeed");
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0], report.manifest);

        let evidence = repositories
            .list_ingest_evidence_for_batch(&submission.batch.id)
            .expect("evidence query should succeed");
        assert_eq!(evidence.len(), 2);

        let snapshots = repositories
            .list_metadata_snapshots_for_batch(&submission.batch.id)
            .expect("snapshot query should succeed");
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.source == MetadataSnapshotSource::GazelleYaml
                && snapshot.format == SnapshotFormat::Yaml
        }));

        let updated_batch = repositories
            .get_import_batch(&submission.batch.id)
            .expect("batch query should succeed")
            .expect("batch should exist");
        assert_eq!(updated_batch.status, ImportBatchStatus::Grouped);

        let _ = std::fs::remove_dir_all(temp_root);
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
                 (id, import_batch_id, release_id, source_id, state, format_family, bitrate_mode,
                  bitrate_kbps, sample_rate_hz, bit_depth, track_count, total_duration_seconds,
                  ingest_origin, import_mode, duplicate_status, export_visibility_policy,
                  original_source_path, imported_at_unix_seconds, gazelle_tracker,
                  gazelle_torrent_id, gazelle_release_group_id)
                 VALUES (?1, ?2, ?3, ?4, 'matched', 'flac', 'lossless', NULL, 44100, 16,
                         10, 2550, 'watch_directory', 'copy', NULL, NULL,
                         '/incoming/radiohead/In Rainbows', 120, 'redacted', '999', '555')",
                params![
                    SeedIds::RELEASE_INSTANCE,
                    SeedIds::IMPORT_BATCH,
                    SeedIds::RELEASE,
                    SeedIds::SOURCE
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO release_instances
                 (id, import_batch_id, release_id, source_id, state, format_family, bitrate_mode,
                  bitrate_kbps, sample_rate_hz, bit_depth, track_count, total_duration_seconds,
                  ingest_origin, import_mode, duplicate_status, export_visibility_policy,
                  original_source_path, imported_at_unix_seconds, gazelle_tracker,
                  gazelle_torrent_id, gazelle_release_group_id)
                 VALUES (?1, ?2, ?3, ?4, 'matched', 'flac', 'constant', 320, 48000, 24,
                         8, 2100, 'manual_add', 'hardlink', NULL, NULL,
                         '/incoming/radiohead-live/Rainbows Live', 220, NULL, NULL, NULL)",
                params![
                    SeedIds::SECOND_RELEASE_INSTANCE,
                    SeedIds::SECOND_IMPORT_BATCH,
                    SeedIds::SECOND_RELEASE,
                    SeedIds::SOURCE
                ],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO tracks
                 (id, release_id, disc_number, track_number, title, normalized_title,
                  musicbrainz_track_id, duration_ms)
                 VALUES (?1, ?2, 1, 1, '15 Step', '15 step',
                         'd1d1d1d1-d1d1-41d1-81d1-d1d1d1d1d1d1', 237000)",
                params![SeedIds::TRACK_ONE, SeedIds::RELEASE],
            )
            .map_err(to_storage_error)?;
        transaction
            .execute(
                "INSERT INTO tracks
                 (id, release_id, disc_number, track_number, title, normalized_title,
                  musicbrainz_track_id, duration_ms)
                 VALUES (?1, ?2, 1, 2, 'Bodysnatchers', 'bodysnatchers',
                         'd2d2d2d2-d2d2-42d2-82d2-d2d2d2d2d2d2', 242000)",
                params![SeedIds::TRACK_TWO, SeedIds::RELEASE],
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
        const TRACK_ONE: &str = "cececece-cece-4ece-8ece-cececececece";
        const TRACK_TWO: &str = "cfcfcfcf-cfcf-4fcf-8fcf-cfcfcfcfcfcf";
        const UNUSED_RELEASE: &str = "dededede-dede-dede-dede-dededededede";
        const UNUSED_ISSUE: &str = "efefefef-efef-efef-efef-efefefefefef";
        const UNUSED_EXPORT: &str = "f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0";
    }
}
