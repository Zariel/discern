use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::api::error::{ApiError, ApiErrorCode};
use crate::api::pagination::ApiPaginationMeta;
use crate::application::config::ValidatedRuntimeConfig;
use crate::application::recovery::{RecoveryError, RecoveryService};
use crate::application::repository::{
    JobListQuery, JobRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType, RetryScope};
use crate::support::ids::JobId;
use crate::support::pagination::PageRequest;

pub type ApiResult<T> = Result<ApiEnvelope<T>, Box<ApiEnvelope<()>>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListJobsRequest {
    pub status: Option<JobStatusValue>,
    pub job_type: Option<JobTypeValue>,
    pub limit: u32,
    pub offset: u64,
}

impl Default for ListJobsRequest {
    fn default() -> Self {
        Self {
            status: None,
            job_type: None,
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryJobRequest {
    pub scope: RetryScopeValue,
    pub queued_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResource {
    pub id: String,
    pub job_type: JobTypeValue,
    pub subject: JobSubjectResource,
    pub status: JobStatusValue,
    pub progress_phase: String,
    pub retry_count: u16,
    pub triggered_by: JobTriggerValue,
    pub created_at_unix_seconds: i64,
    pub started_at_unix_seconds: Option<i64>,
    pub finished_at_unix_seconds: Option<i64>,
    pub error_payload: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSubjectResource {
    pub kind: String,
    pub reference: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatusValue {
    Queued,
    Running,
    Succeeded,
    Failed,
    Resumable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobTypeValue {
    DiscoverBatch,
    AnalyzeReleaseInstance,
    MatchReleaseInstance,
    EnrichReleaseInstance,
    RenderExportMetadata,
    WriteTags,
    OrganizeFiles,
    VerifyImport,
    ReprocessReleaseInstance,
    RescanWatcher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobTriggerValue {
    System,
    Operator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryScopeValue {
    Reanalyze,
    Rematch,
    RerenderExport,
    Retag,
    Reorganize,
    FullReprocess,
}

pub struct JobsApi<R> {
    repository: R,
    config: ValidatedRuntimeConfig,
}

impl<R> JobsApi<R> {
    pub fn new(repository: R, config: ValidatedRuntimeConfig) -> Self {
        Self { repository, config }
    }
}

impl<R> JobsApi<R>
where
    R: Clone
        + JobRepository
        + crate::application::repository::JobCommandRepository
        + crate::application::repository::ImportBatchCommandRepository
        + crate::application::repository::ImportBatchRepository
        + crate::application::repository::IssueCommandRepository
        + crate::application::repository::IssueRepository
        + crate::application::repository::ReleaseInstanceCommandRepository
        + crate::application::repository::ReleaseInstanceRepository
        + crate::application::repository::SourceCommandRepository
        + crate::application::repository::SourceRepository
        + crate::application::repository::StagingManifestCommandRepository
        + crate::application::repository::StagingManifestRepository,
{
    pub fn list_jobs(
        &self,
        request_id: impl Into<String>,
        request: ListJobsRequest,
    ) -> ApiResult<Vec<JobResource>> {
        let request_id = request_id.into();
        let page = self
            .repository
            .list_jobs(&JobListQuery {
                status: request.status.map(Into::into),
                job_type: request.job_type.map(Into::into),
                page: PageRequest::new(request.limit, request.offset),
            })
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success_with_pagination(
            page.items.iter().map(JobResource::from).collect(),
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }

    pub fn get_job(&self, request_id: impl Into<String>, job_id: &str) -> ApiResult<JobResource> {
        let request_id = request_id.into();
        let job_id = parse_job_id(job_id, &request_id)?;
        let job = self
            .repository
            .get_job(&job_id)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?
            .ok_or_else(|| {
                ApiEnvelope::error(
                    ApiError::new(
                        ApiErrorCode::NotFound,
                        format!("job {} was not found", job_id.as_uuid()),
                        None,
                    ),
                    request_id.clone(),
                )
            })?;
        Ok(ApiEnvelope::success(JobResource::from(&job), request_id))
    }

    pub fn retry_job(
        &self,
        request_id: impl Into<String>,
        job_id: &str,
        request: RetryJobRequest,
    ) -> ApiResult<JobResource> {
        let request_id = request_id.into();
        let job_id = parse_job_id(job_id, &request_id)?;
        let job = RecoveryService::new(self.repository.clone(), self.config.clone())
            .retry_job(
                &job_id,
                request.scope.into(),
                request.queued_at_unix_seconds,
            )
            .map_err(|error| recovery_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(JobResource::from(&job), request_id))
    }
}

impl From<&Job> for JobResource {
    fn from(job: &Job) -> Self {
        Self {
            id: job.id.as_uuid().to_string(),
            job_type: job.job_type.clone().into(),
            subject: JobSubjectResource::from(&job.subject),
            status: job.status.clone().into(),
            progress_phase: job.progress_phase.clone(),
            retry_count: job.retry_count,
            triggered_by: job.triggered_by.clone().into(),
            created_at_unix_seconds: job.created_at_unix_seconds,
            started_at_unix_seconds: job.started_at_unix_seconds,
            finished_at_unix_seconds: job.finished_at_unix_seconds,
            error_payload: job.error_payload.clone(),
        }
    }
}

impl From<&JobSubject> for JobSubjectResource {
    fn from(subject: &JobSubject) -> Self {
        match subject {
            JobSubject::ImportBatch(batch_id) => Self {
                kind: "import_batch".to_string(),
                reference: batch_id.as_uuid().to_string(),
            },
            JobSubject::ReleaseInstance(release_instance_id) => Self {
                kind: "release_instance".to_string(),
                reference: release_instance_id.as_uuid().to_string(),
            },
            JobSubject::SourceScan(scan_subject) => Self {
                kind: "source_scan".to_string(),
                reference: scan_subject.clone(),
            },
        }
    }
}

impl From<JobStatus> for JobStatusValue {
    fn from(value: JobStatus) -> Self {
        match value {
            JobStatus::Queued => Self::Queued,
            JobStatus::Running => Self::Running,
            JobStatus::Succeeded => Self::Succeeded,
            JobStatus::Failed => Self::Failed,
            JobStatus::Resumable => Self::Resumable,
        }
    }
}

impl From<JobStatusValue> for JobStatus {
    fn from(value: JobStatusValue) -> Self {
        match value {
            JobStatusValue::Queued => Self::Queued,
            JobStatusValue::Running => Self::Running,
            JobStatusValue::Succeeded => Self::Succeeded,
            JobStatusValue::Failed => Self::Failed,
            JobStatusValue::Resumable => Self::Resumable,
        }
    }
}

impl From<JobType> for JobTypeValue {
    fn from(value: JobType) -> Self {
        match value {
            JobType::DiscoverBatch => Self::DiscoverBatch,
            JobType::AnalyzeReleaseInstance => Self::AnalyzeReleaseInstance,
            JobType::MatchReleaseInstance => Self::MatchReleaseInstance,
            JobType::EnrichReleaseInstance => Self::EnrichReleaseInstance,
            JobType::RenderExportMetadata => Self::RenderExportMetadata,
            JobType::WriteTags => Self::WriteTags,
            JobType::OrganizeFiles => Self::OrganizeFiles,
            JobType::VerifyImport => Self::VerifyImport,
            JobType::ReprocessReleaseInstance => Self::ReprocessReleaseInstance,
            JobType::RescanWatcher => Self::RescanWatcher,
        }
    }
}

impl From<JobTypeValue> for JobType {
    fn from(value: JobTypeValue) -> Self {
        match value {
            JobTypeValue::DiscoverBatch => Self::DiscoverBatch,
            JobTypeValue::AnalyzeReleaseInstance => Self::AnalyzeReleaseInstance,
            JobTypeValue::MatchReleaseInstance => Self::MatchReleaseInstance,
            JobTypeValue::EnrichReleaseInstance => Self::EnrichReleaseInstance,
            JobTypeValue::RenderExportMetadata => Self::RenderExportMetadata,
            JobTypeValue::WriteTags => Self::WriteTags,
            JobTypeValue::OrganizeFiles => Self::OrganizeFiles,
            JobTypeValue::VerifyImport => Self::VerifyImport,
            JobTypeValue::ReprocessReleaseInstance => Self::ReprocessReleaseInstance,
            JobTypeValue::RescanWatcher => Self::RescanWatcher,
        }
    }
}

impl From<JobTrigger> for JobTriggerValue {
    fn from(value: JobTrigger) -> Self {
        match value {
            JobTrigger::System => Self::System,
            JobTrigger::Operator => Self::Operator,
        }
    }
}

impl From<RetryScopeValue> for RetryScope {
    fn from(value: RetryScopeValue) -> Self {
        match value {
            RetryScopeValue::Reanalyze => Self::Reanalyze,
            RetryScopeValue::Rematch => Self::Rematch,
            RetryScopeValue::RerenderExport => Self::RerenderExport,
            RetryScopeValue::Retag => Self::Retag,
            RetryScopeValue::Reorganize => Self::Reorganize,
            RetryScopeValue::FullReprocess => Self::FullReprocess,
        }
    }
}

fn parse_job_id(job_id: &str, request_id: &str) -> Result<JobId, Box<ApiEnvelope<()>>> {
    JobId::parse_str(job_id).map_err(|_| {
        Box::new(ApiEnvelope::error(
            ApiError::new(
                ApiErrorCode::InvalidRequest,
                format!("job id '{job_id}' is not a valid UUID"),
                None,
            ),
            request_id.to_string(),
        ))
    })
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
    use std::sync::{Arc, Mutex};

    use crate::application::repository::{
        ImportBatchCommandRepository, ImportBatchListQuery, ImportBatchRepository,
        IssueCommandRepository, IssueListQuery, IssueRepository, ReleaseInstanceCommandRepository,
        ReleaseInstanceListQuery, ReleaseInstanceRepository, RepositoryError,
        SourceCommandRepository, SourceRepository, StagingManifestCommandRepository,
        StagingManifestRepository,
    };
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::import_batch::ImportBatch;
    use crate::domain::issue::Issue;
    use crate::domain::release_instance::ReleaseInstance;
    use crate::domain::source::{Source, SourceLocator};
    use crate::domain::staging_manifest::StagingManifest;
    use crate::domain::track_instance::TrackInstance;
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn list_jobs_returns_paginated_resources() {
        let repository = InMemoryJobsApiRepository::default();
        repository.insert_job(Job::queued(
            JobType::MatchReleaseInstance,
            JobSubject::SourceScan("watch".to_string()),
            JobTrigger::System,
            100,
        ));
        repository.insert_job(Job::queued(
            JobType::VerifyImport,
            JobSubject::SourceScan("verify".to_string()),
            JobTrigger::Operator,
            110,
        ));
        let api = JobsApi::new(
            repository,
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default()),
        );

        let envelope = api
            .list_jobs(
                "req_jobs",
                ListJobsRequest {
                    limit: 1,
                    offset: 0,
                    ..ListJobsRequest::default()
                },
            )
            .expect("job list should succeed");

        assert_eq!(envelope.data.as_ref().expect("data should exist").len(), 1);
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
    fn retry_job_returns_updated_resource() {
        let repository = InMemoryJobsApiRepository::default();
        let job = repository.insert_job(Job::queued(
            JobType::WriteTags,
            JobSubject::SourceScan("watch".to_string()),
            JobTrigger::Operator,
            100,
        ));
        let service = crate::application::jobs::JobService::new(repository.clone());
        service
            .start_job(&job.id, "tagging", 101)
            .expect("job should start");
        service
            .fail_job(&job.id, "tagging", "disk full", 102)
            .expect("job should fail");

        let api = JobsApi::new(
            repository.clone(),
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default()),
        );
        let envelope = api
            .retry_job(
                "req_retry",
                &job.id.as_uuid().to_string(),
                RetryJobRequest {
                    scope: RetryScopeValue::Retag,
                    queued_at_unix_seconds: 103,
                },
            )
            .expect("retry should succeed");

        let resource = envelope.data.expect("data should exist");
        assert_eq!(resource.status, JobStatusValue::Queued);
        assert_eq!(resource.retry_count, 1);
    }

    #[derive(Clone, Default)]
    struct InMemoryJobsApiRepository {
        jobs: Arc<Mutex<HashMap<String, Job>>>,
    }

    impl InMemoryJobsApiRepository {
        fn insert_job(&self, job: Job) -> Job {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            job
        }
    }

    impl JobRepository for InMemoryJobsApiRepository {
        fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_jobs(&self, query: &JobListQuery) -> Result<Page<Job>, RepositoryError> {
            let mut items = self
                .jobs
                .lock()
                .expect("jobs should lock")
                .values()
                .filter(|job| {
                    query
                        .status
                        .as_ref()
                        .is_none_or(|status| &job.status == status)
                })
                .filter(|job| {
                    query
                        .job_type
                        .as_ref()
                        .is_none_or(|job_type| &job.job_type == job_type)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by_key(|job| job.created_at_unix_seconds);
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

    impl crate::application::repository::JobCommandRepository for InMemoryJobsApiRepository {
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

    impl ImportBatchRepository for InMemoryJobsApiRepository {
        fn get_import_batch(
            &self,
            _id: &crate::support::ids::ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok(None)
        }

        fn list_import_batches(
            &self,
            _query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::default(),
                total: 0,
            })
        }
    }

    impl ImportBatchCommandRepository for InMemoryJobsApiRepository {
        fn create_import_batch(&self, _batch: &ImportBatch) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_import_batch(&self, _batch: &ImportBatch) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn list_active_import_batches_for_source(
            &self,
            _source_id: &crate::support::ids::SourceId,
        ) -> Result<Vec<ImportBatch>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl IssueRepository for InMemoryJobsApiRepository {
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

    impl IssueCommandRepository for InMemoryJobsApiRepository {
        fn create_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl ReleaseInstanceRepository for InMemoryJobsApiRepository {
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

    impl ReleaseInstanceCommandRepository for InMemoryJobsApiRepository {
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

    impl SourceRepository for InMemoryJobsApiRepository {
        fn get_source(
            &self,
            _id: &crate::support::ids::SourceId,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }

        fn find_source_by_locator(
            &self,
            _locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }
    }

    impl SourceCommandRepository for InMemoryJobsApiRepository {
        fn create_source(&self, _source: &Source) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl StagingManifestRepository for InMemoryJobsApiRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl StagingManifestCommandRepository for InMemoryJobsApiRepository {
        fn create_staging_manifest(
            &self,
            _manifest: &StagingManifest,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }
}
