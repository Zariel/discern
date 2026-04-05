use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::api::error::{ApiError, ApiErrorCode};
use crate::api::jobs::JobResource;
use crate::api::pagination::ApiPaginationMeta;
use crate::application::config::ValidatedRuntimeConfig;
use crate::application::ingest::{
    IngestSubmissionReport, WatchDiscoveryError, WatchDiscoveryService,
};
use crate::application::recovery::{RecoveryError, RecoveryService};
use crate::application::repository::{
    ImportBatchCommandRepository, ImportBatchListQuery, ImportBatchRepository,
    JobCommandRepository, RepositoryError, RepositoryErrorKind, SourceCommandRepository,
    SourceRepository,
};
use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
use crate::domain::source::{Source, SourceKind, SourceLocator};
use crate::support::pagination::PageRequest;

pub type ApiResult<T> = Result<ApiEnvelope<T>, Box<ApiEnvelope<()>>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateImportBatchRequest {
    pub client_name: String,
    pub submitted_paths: Vec<String>,
    pub submitted_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateImportBatchFromPathRequest {
    pub operator_name: String,
    pub submitted_path: String,
    pub submitted_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RescanWatcherRequest {
    pub watcher: String,
    pub discovered_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListImportBatchesRequest {
    pub limit: u32,
    pub offset: u64,
}

impl Default for ListImportBatchesRequest {
    fn default() -> Self {
        Self {
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportSubmissionResource {
    pub source: SourceResource,
    pub batch: ImportBatchResource,
    pub job: JobResource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchRescanResource {
    pub created_batches: Vec<ImportBatchResource>,
    pub queued_jobs: Vec<JobResource>,
    pub skipped_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportBatchResource {
    pub id: String,
    pub source_id: String,
    pub mode: ImportModeValue,
    pub status: ImportBatchStatusValue,
    pub requested_by: BatchRequesterResource,
    pub created_at_unix_seconds: i64,
    pub received_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceResource {
    pub id: String,
    pub kind: SourceKindValue,
    pub display_name: String,
    pub locator: SourceLocatorResource,
    pub external_reference: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLocatorResource {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchRequesterResource {
    pub kind: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportModeValue {
    Copy,
    Move,
    Hardlink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportBatchStatusValue {
    Created,
    Discovering,
    Grouped,
    Submitted,
    Quarantined,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKindValue {
    WatchDirectory,
    ApiClient,
    ManualAdd,
    Gazelle,
}

pub struct IngestApi<R> {
    repository: R,
    config: ValidatedRuntimeConfig,
}

impl<R> IngestApi<R> {
    pub fn new(repository: R, config: ValidatedRuntimeConfig) -> Self {
        Self { repository, config }
    }
}

impl<R> IngestApi<R>
where
    R: Clone
        + SourceRepository
        + SourceCommandRepository
        + ImportBatchCommandRepository
        + ImportBatchRepository
        + JobCommandRepository
        + crate::application::repository::JobRepository,
{
    pub fn create_import_batch(
        &self,
        request_id: impl Into<String>,
        request: CreateImportBatchRequest,
    ) -> ApiResult<ImportSubmissionResource> {
        let request_id = request_id.into();
        let report = WatchDiscoveryService::new(self.repository.clone(), self.config.clone())
            .submit_api_paths(
                request.client_name,
                request
                    .submitted_paths
                    .into_iter()
                    .map(PathBuf::from)
                    .collect(),
                request.submitted_at_unix_seconds,
            )
            .map_err(|error| watch_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            ImportSubmissionResource::from(&report),
            request_id,
        ))
    }

    pub fn create_import_batch_from_path(
        &self,
        request_id: impl Into<String>,
        request: CreateImportBatchFromPathRequest,
    ) -> ApiResult<ImportSubmissionResource> {
        let request_id = request_id.into();
        let report = WatchDiscoveryService::new(self.repository.clone(), self.config.clone())
            .submit_manual_path(
                request.operator_name,
                PathBuf::from(request.submitted_path),
                request.submitted_at_unix_seconds,
            )
            .map_err(|error| watch_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            ImportSubmissionResource::from(&report),
            request_id,
        ))
    }

    pub fn rescan_watcher(
        &self,
        request_id: impl Into<String>,
        request: RescanWatcherRequest,
    ) -> ApiResult<WatchRescanResource>
    where
        R: crate::application::repository::IssueCommandRepository
            + crate::application::repository::IssueRepository
            + crate::application::repository::ReleaseInstanceCommandRepository
            + crate::application::repository::ReleaseInstanceRepository
            + crate::application::repository::StagingManifestCommandRepository
            + crate::application::repository::StagingManifestRepository,
    {
        let request_id = request_id.into();
        let report = RecoveryService::new(self.repository.clone(), self.config.clone())
            .rescan_watcher(&request.watcher, request.discovered_at_unix_seconds)
            .map_err(|error| recovery_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            WatchRescanResource {
                created_batches: report
                    .created_batches
                    .iter()
                    .map(ImportBatchResource::from)
                    .collect(),
                queued_jobs: report.queued_jobs.iter().map(JobResource::from).collect(),
                skipped_paths: report
                    .skipped_paths
                    .iter()
                    .map(|path: &PathBuf| path.display().to_string())
                    .collect(),
            },
            request_id,
        ))
    }

    pub fn list_import_batches(
        &self,
        request_id: impl Into<String>,
        request: ListImportBatchesRequest,
    ) -> ApiResult<Vec<ImportBatchResource>> {
        let request_id = request_id.into();
        let page = self
            .repository
            .list_import_batches(&ImportBatchListQuery {
                page: PageRequest::new(request.limit, request.offset),
            })
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success_with_pagination(
            page.items.iter().map(ImportBatchResource::from).collect(),
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }
}

impl From<&IngestSubmissionReport> for ImportSubmissionResource {
    fn from(report: &IngestSubmissionReport) -> Self {
        Self {
            source: SourceResource::from(&report.source),
            batch: ImportBatchResource::from(&report.batch),
            job: JobResource::from(&report.job),
        }
    }
}

impl From<&ImportBatch> for ImportBatchResource {
    fn from(batch: &ImportBatch) -> Self {
        Self {
            id: batch.id.as_uuid().to_string(),
            source_id: batch.source_id.as_uuid().to_string(),
            mode: batch.mode.clone().into(),
            status: batch.status.clone().into(),
            requested_by: BatchRequesterResource::from(&batch.requested_by),
            created_at_unix_seconds: batch.created_at_unix_seconds,
            received_paths: batch
                .received_paths
                .iter()
                .map(|path| path.display().to_string())
                .collect(),
        }
    }
}

impl From<&Source> for SourceResource {
    fn from(source: &Source) -> Self {
        Self {
            id: source.id.as_uuid().to_string(),
            kind: source.kind.clone().into(),
            display_name: source.display_name.clone(),
            locator: SourceLocatorResource::from(&source.locator),
            external_reference: source.external_reference.clone(),
        }
    }
}

impl From<&SourceLocator> for SourceLocatorResource {
    fn from(locator: &SourceLocator) -> Self {
        match locator {
            SourceLocator::FilesystemPath(path) => Self {
                kind: "filesystem_path".to_string(),
                value: path.display().to_string(),
            },
            SourceLocator::ApiClient { client_name } => Self {
                kind: "api_client".to_string(),
                value: client_name.clone(),
            },
            SourceLocator::ManualEntry { submitted_path } => Self {
                kind: "manual_entry".to_string(),
                value: submitted_path.display().to_string(),
            },
            SourceLocator::TrackerRef {
                tracker,
                identifier,
            } => Self {
                kind: "tracker_ref".to_string(),
                value: format!("{tracker}:{identifier}"),
            },
        }
    }
}

impl From<&BatchRequester> for BatchRequesterResource {
    fn from(value: &BatchRequester) -> Self {
        match value {
            BatchRequester::System => Self {
                kind: "system".to_string(),
                name: None,
            },
            BatchRequester::Operator { name } => Self {
                kind: "operator".to_string(),
                name: Some(name.clone()),
            },
            BatchRequester::ExternalClient { name } => Self {
                kind: "external_client".to_string(),
                name: Some(name.clone()),
            },
        }
    }
}

impl From<ImportMode> for ImportModeValue {
    fn from(value: ImportMode) -> Self {
        match value {
            ImportMode::Copy => Self::Copy,
            ImportMode::Move => Self::Move,
            ImportMode::Hardlink => Self::Hardlink,
        }
    }
}

impl From<ImportBatchStatus> for ImportBatchStatusValue {
    fn from(value: ImportBatchStatus) -> Self {
        match value {
            ImportBatchStatus::Created => Self::Created,
            ImportBatchStatus::Discovering => Self::Discovering,
            ImportBatchStatus::Grouped => Self::Grouped,
            ImportBatchStatus::Submitted => Self::Submitted,
            ImportBatchStatus::Quarantined => Self::Quarantined,
            ImportBatchStatus::Failed => Self::Failed,
        }
    }
}

impl From<SourceKind> for SourceKindValue {
    fn from(value: SourceKind) -> Self {
        match value {
            SourceKind::WatchDirectory => Self::WatchDirectory,
            SourceKind::ApiClient => Self::ApiClient,
            SourceKind::ManualAdd => Self::ManualAdd,
            SourceKind::Gazelle => Self::Gazelle,
        }
    }
}

fn repository_error_envelope(error: RepositoryError, request_id: String) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                RepositoryErrorKind::NotFound => ApiErrorCode::NotFound,
                RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                    ApiErrorCode::Conflict
                }
                RepositoryErrorKind::Storage => ApiErrorCode::InternalError,
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

fn watch_error_envelope(error: WatchDiscoveryError, request_id: String) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                crate::application::ingest::WatchDiscoveryErrorKind::NotFound => {
                    ApiErrorCode::NotFound
                }
                crate::application::ingest::WatchDiscoveryErrorKind::Conflict => {
                    ApiErrorCode::Conflict
                }
                crate::application::ingest::WatchDiscoveryErrorKind::Storage
                | crate::application::ingest::WatchDiscoveryErrorKind::Io => {
                    ApiErrorCode::InternalError
                }
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

fn recovery_error_envelope(error: RecoveryError, request_id: String) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                crate::application::recovery::RecoveryErrorKind::NotFound => ApiErrorCode::NotFound,
                crate::application::recovery::RecoveryErrorKind::Conflict => ApiErrorCode::Conflict,
                crate::application::recovery::RecoveryErrorKind::Storage => {
                    ApiErrorCode::InternalError
                }
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use id3::TagLike;

    use crate::api::jobs::JobTypeValue;
    use crate::application::repository::{
        IssueCommandRepository, IssueListQuery, IssueRepository, JobRepository,
        ReleaseInstanceCommandRepository, ReleaseInstanceListQuery, ReleaseInstanceRepository,
        RepositoryError, StagingManifestCommandRepository, StagingManifestRepository,
    };
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::issue::Issue;
    use crate::domain::job::Job;
    use crate::domain::release_instance::ReleaseInstance;
    use crate::domain::staging_manifest::StagingManifest;
    use crate::domain::track_instance::TrackInstance;
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn create_import_batch_returns_submission_resource() {
        let repository = InMemoryIngestApiRepository::default();
        let api = IngestApi::new(
            repository.clone(),
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default()),
        );

        let envelope = api
            .create_import_batch(
                "req_ingest",
                CreateImportBatchRequest {
                    client_name: "lidarr".to_string(),
                    submitted_paths: vec!["/imports/drop".to_string()],
                    submitted_at_unix_seconds: 200,
                },
            )
            .expect("ingest should succeed");

        let data = envelope.data.expect("data should exist");
        assert_eq!(data.source.kind, SourceKindValue::ApiClient);
        assert_eq!(data.batch.received_paths, vec!["/imports/drop".to_string()]);
        assert_eq!(data.job.job_type, JobTypeValue::DiscoverBatch);
    }

    #[test]
    fn list_import_batches_returns_paginated_resources() {
        let repository = InMemoryIngestApiRepository::default();
        repository.seed_batch("/imports/one");
        repository.seed_batch("/imports/two");
        let api = IngestApi::new(
            repository,
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default()),
        );

        let envelope = api
            .list_import_batches(
                "req_batches",
                ListImportBatchesRequest {
                    limit: 1,
                    offset: 0,
                },
            )
            .expect("list should succeed");

        assert_eq!(envelope.data.expect("data should exist").len(), 1);
        assert_eq!(
            envelope
                .meta
                .pagination
                .expect("pagination should exist")
                .next_offset,
            Some(1)
        );
    }

    #[test]
    fn rescan_watcher_returns_discovered_jobs() {
        let root = test_root("api-rescan");
        let watch_dir = root.join("watch");
        let album_dir = watch_dir.join("Kid A");
        fs::create_dir_all(&album_dir).expect("album dir should exist");
        let mp3_path = album_dir.join("01 - Everything in Its Right Place.mp3");
        seed_mp3(
            &mp3_path,
            "Radiohead",
            "Kid A",
            "Everything in Its Right Place",
        );

        let mut config = crate::config::AppConfig::default();
        config.storage.watch_directories = vec![crate::config::WatchDirectoryConfig {
            name: "watch".to_string(),
            path: watch_dir.clone(),
            scan_mode: crate::config::WatchScanMode::EventDriven,
            import_mode_override: None,
        }];
        let repository = InMemoryIngestApiRepository::default();
        let api = IngestApi::new(
            repository,
            ValidatedRuntimeConfig::from_validated_app_config(&config),
        );

        let envelope = api
            .rescan_watcher(
                "req_rescan",
                RescanWatcherRequest {
                    watcher: "watch".to_string(),
                    discovered_at_unix_seconds: 300,
                },
            )
            .expect("rescan should succeed");

        let data = envelope.data.expect("data should exist");
        assert_eq!(data.created_batches.len(), 1);
        assert_eq!(data.queued_jobs.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[derive(Clone, Default)]
    struct InMemoryIngestApiRepository {
        sources: Arc<Mutex<HashMap<String, Source>>>,
        batches: Arc<Mutex<HashMap<String, ImportBatch>>>,
        jobs: Arc<Mutex<HashMap<String, Job>>>,
    }

    impl InMemoryIngestApiRepository {
        fn seed_batch(&self, path: &str) {
            let source_id = crate::support::ids::SourceId::new();
            let batch = ImportBatch {
                id: crate::support::ids::ImportBatchId::new(),
                source_id,
                mode: ImportMode::Copy,
                status: ImportBatchStatus::Created,
                requested_by: BatchRequester::System,
                created_at_unix_seconds: 100,
                received_paths: vec![PathBuf::from(path)],
            };
            self.batches
                .lock()
                .expect("batches should lock")
                .insert(batch.id.as_uuid().to_string(), batch);
        }
    }

    impl SourceRepository for InMemoryIngestApiRepository {
        fn get_source(
            &self,
            id: &crate::support::ids::SourceId,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(self
                .sources
                .lock()
                .expect("sources should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn find_source_by_locator(
            &self,
            locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(self
                .sources
                .lock()
                .expect("sources should lock")
                .values()
                .find(|source| &source.locator == locator)
                .cloned())
        }
    }

    impl SourceCommandRepository for InMemoryIngestApiRepository {
        fn create_source(&self, source: &Source) -> Result<(), RepositoryError> {
            self.sources
                .lock()
                .expect("sources should lock")
                .insert(source.id.as_uuid().to_string(), source.clone());
            Ok(())
        }
    }

    impl ImportBatchRepository for InMemoryIngestApiRepository {
        fn get_import_batch(
            &self,
            id: &crate::support::ids::ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok(self
                .batches
                .lock()
                .expect("batches should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_import_batches(
            &self,
            query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            let mut items = self
                .batches
                .lock()
                .expect("batches should lock")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by_key(|batch| batch.created_at_unix_seconds);
            let total = items.len() as u64;
            let items = items
                .into_iter()
                .skip(query.page.offset as usize)
                .take(query.page.limit as usize)
                .collect();
            Ok(Page {
                items,
                request: query.page,
                total,
            })
        }
    }

    impl ImportBatchCommandRepository for InMemoryIngestApiRepository {
        fn create_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
            self.batches
                .lock()
                .expect("batches should lock")
                .insert(batch.id.as_uuid().to_string(), batch.clone());
            Ok(())
        }

        fn update_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
            self.batches
                .lock()
                .expect("batches should lock")
                .insert(batch.id.as_uuid().to_string(), batch.clone());
            Ok(())
        }

        fn list_active_import_batches_for_source(
            &self,
            source_id: &crate::support::ids::SourceId,
        ) -> Result<Vec<ImportBatch>, RepositoryError> {
            Ok(self
                .batches
                .lock()
                .expect("batches should lock")
                .values()
                .filter(|batch| &batch.source_id == source_id)
                .filter(|batch| {
                    matches!(
                        batch.status,
                        ImportBatchStatus::Created
                            | ImportBatchStatus::Discovering
                            | ImportBatchStatus::Grouped
                            | ImportBatchStatus::Submitted
                    )
                })
                .cloned()
                .collect())
        }
    }

    impl crate::application::repository::JobCommandRepository for InMemoryIngestApiRepository {
        fn create_job(&self, job: &Job) -> Result<(), RepositoryError> {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            Ok(())
        }

        fn update_job(&self, job: &Job) -> Result<(), RepositoryError> {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            Ok(())
        }

        fn list_recoverable_jobs(&self) -> Result<Vec<Job>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl JobRepository for InMemoryIngestApiRepository {
        fn get_job(&self, id: &crate::support::ids::JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_jobs(
            &self,
            _query: &crate::application::repository::JobListQuery,
        ) -> Result<Page<Job>, RepositoryError> {
            Ok(Page {
                items: self
                    .jobs
                    .lock()
                    .expect("jobs should lock")
                    .values()
                    .cloned()
                    .collect(),
                request: PageRequest::default(),
                total: self.jobs.lock().expect("jobs should lock").len() as u64,
            })
        }
    }

    impl IssueRepository for InMemoryIngestApiRepository {
        fn get_issue(
            &self,
            _id: &crate::support::ids::IssueId,
        ) -> Result<Option<Issue>, RepositoryError> {
            Ok(None)
        }

        fn list_issues(&self, _query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }
    }

    impl IssueCommandRepository for InMemoryIngestApiRepository {
        fn create_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl ReleaseInstanceRepository for InMemoryIngestApiRepository {
        fn get_release_instance(
            &self,
            _id: &crate::support::ids::ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(None)
        }

        fn list_release_instances(
            &self,
            _query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &crate::support::ids::ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            Ok(None)
        }

        fn list_track_instances_for_release_instance(
            &self,
            _release_instance_id: &crate::support::ids::ReleaseInstanceId,
        ) -> Result<Vec<TrackInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &crate::support::ids::ReleaseInstanceId,
            _role: Option<FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryIngestApiRepository {
        fn create_release_instance(
            &self,
            _release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_release_instance(
            &self,
            _release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_candidate_matches(
            &self,
            _release_instance_id: &crate::support::ids::ReleaseInstanceId,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_candidate_matches_for_provider(
            &self,
            _release_instance_id: &crate::support::ids::ReleaseInstanceId,
            _provider: &crate::domain::candidate_match::CandidateProvider,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_track_instances_and_files(
            &self,
            _release_instance_id: &crate::support::ids::ReleaseInstanceId,
            _track_instances: &[TrackInstance],
            _files: &[FileRecord],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl StagingManifestRepository for InMemoryIngestApiRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl StagingManifestCommandRepository for InMemoryIngestApiRepository {
        fn create_staging_manifest(
            &self,
            _manifest: &StagingManifest,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    fn test_root(label: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("discern-api-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("temp root should exist");
        root
    }

    fn seed_mp3(path: &Path, artist: &str, album: &str, title: &str) {
        std::fs::write(path, []).expect("mp3 fixture should exist");
        let mut tag = id3::Tag::new();
        tag.set_artist(artist);
        tag.set_album(album);
        tag.set_title(title);
        tag.write_to_path(path, id3::Version::Id3v24)
            .expect("mp3 tag should write");
    }
}
