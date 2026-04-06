use crate::api::issues::{
    IssueDetailResource, IssueResource, IssueStateValue, IssueTypeValue, IssuesApi,
    ListIssuesRequest, SuppressIssueRequest,
};
use crate::api::jobs::{
    JobResource, JobStatusValue, JobTypeValue, JobsApi, ListJobsRequest, RetryJobRequest,
    RetryScopeValue,
};
use crate::application::config::ValidatedRuntimeConfig;
use crate::application::repository::{
    ExportRepository, ImportBatchCommandRepository, ImportBatchRepository, IssueCommandRepository,
    IssueRepository, JobCommandRepository, JobRepository, ReleaseInstanceCommandRepository,
    ReleaseInstanceRepository, SourceCommandRepository, SourceRepository,
    StagingManifestCommandRepository, StagingManifestRepository,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IssueQueueFilters {
    pub issue_type: Option<IssueTypeValue>,
    pub state: Option<IssueStateValue>,
    pub selected_issue_id: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueQueueSummary {
    pub total: u64,
    pub open_count: usize,
    pub selected_issue_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueQueueScreen {
    pub filters: IssueQueueFilters,
    pub summary: IssueQueueSummary,
    pub items: Vec<IssueResource>,
    pub selected_issue: Option<IssueDetailResource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueQueueActionRequest {
    Resolve {
        issue_id: String,
        resolved_at_unix_seconds: i64,
    },
    Suppress {
        issue_id: String,
        reason: String,
        suppressed_at_unix_seconds: i64,
    },
}

pub struct IssueQueueScreenLoader<R> {
    repository: R,
}

impl<R> IssueQueueScreenLoader<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> IssueQueueScreenLoader<R>
where
    R: Clone + IssueRepository + IssueCommandRepository + ExportRepository,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        filters: IssueQueueFilters,
    ) -> Result<IssueQueueScreen, String> {
        let request_id = request_id.into();
        let request = ListIssuesRequest {
            state: filters.state,
            issue_type: filters.issue_type.clone(),
            limit: normalize_limit(filters.limit),
            offset: filters.offset,
        };
        let issues = IssuesApi::new(self.repository.clone())
            .list_issues(request_id.clone(), request)
            .map_err(|envelope| error_message(*envelope))?
            .clone();
        let meta = issues.meta.pagination.clone();
        let items = issues.data.unwrap_or_default();
        let selected_issue = match filters.selected_issue_id.as_deref() {
            Some(issue_id) => Some(
                IssuesApi::new(self.repository.clone())
                    .get_issue(request_id, issue_id)
                    .map_err(|envelope| error_message(*envelope))?
                    .data
                    .expect("issue detail data should exist"),
            ),
            None => None,
        };
        Ok(IssueQueueScreen {
            summary: IssueQueueSummary {
                total: meta.map(|value| value.total).unwrap_or(items.len() as u64),
                open_count: items
                    .iter()
                    .filter(|issue| issue.state == IssueStateValue::Open)
                    .count(),
                selected_issue_id: filters.selected_issue_id.clone(),
            },
            filters,
            items,
            selected_issue,
        })
    }

    pub fn act(
        &self,
        request_id: impl Into<String>,
        action: IssueQueueActionRequest,
    ) -> Result<IssueDetailResource, String> {
        let request_id = request_id.into();
        let api = IssuesApi::new(self.repository.clone());
        match action {
            IssueQueueActionRequest::Resolve {
                issue_id,
                resolved_at_unix_seconds,
            } => {
                let issue = api
                    .resolve_issue(request_id.clone(), &issue_id, resolved_at_unix_seconds)
                    .map_err(|envelope| error_message(*envelope))?
                    .data
                    .expect("issue resource should exist");
                api.get_issue(request_id, &issue.id)
                    .map_err(|envelope| error_message(*envelope))?
                    .data
                    .ok_or_else(|| "issue detail response was empty".to_string())
            }
            IssueQueueActionRequest::Suppress {
                issue_id,
                reason,
                suppressed_at_unix_seconds,
            } => {
                let issue = api
                    .suppress_issue(
                        request_id.clone(),
                        &issue_id,
                        SuppressIssueRequest {
                            reason,
                            suppressed_at_unix_seconds,
                        },
                    )
                    .map_err(|envelope| error_message(*envelope))?
                    .data
                    .expect("issue resource should exist");
                api.get_issue(request_id, &issue.id)
                    .map_err(|envelope| error_message(*envelope))?
                    .data
                    .ok_or_else(|| "issue detail response was empty".to_string())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobsScreenFilters {
    pub status: Option<JobStatusValue>,
    pub job_type: Option<JobTypeValue>,
    pub selected_job_id: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobsScreenSummary {
    pub total: u64,
    pub running_count: usize,
    pub failed_count: usize,
    pub resumable_count: usize,
    pub selected_job_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobsScreen {
    pub filters: JobsScreenFilters,
    pub summary: JobsScreenSummary,
    pub items: Vec<JobResource>,
    pub selected_job: Option<JobResource>,
}

pub struct JobsScreenLoader<R> {
    repository: R,
    config: ValidatedRuntimeConfig,
}

impl<R> JobsScreenLoader<R> {
    pub fn new(repository: R, config: ValidatedRuntimeConfig) -> Self {
        Self { repository, config }
    }
}

impl<R> JobsScreenLoader<R>
where
    R: Clone
        + JobRepository
        + JobCommandRepository
        + ImportBatchCommandRepository
        + ImportBatchRepository
        + IssueCommandRepository
        + IssueRepository
        + ReleaseInstanceCommandRepository
        + ReleaseInstanceRepository
        + SourceCommandRepository
        + SourceRepository
        + StagingManifestCommandRepository
        + StagingManifestRepository,
{
    pub fn load(
        &self,
        request_id: impl Into<String>,
        filters: JobsScreenFilters,
    ) -> Result<JobsScreen, String> {
        let request_id = request_id.into();
        let request = ListJobsRequest {
            status: filters.status,
            job_type: filters.job_type,
            limit: normalize_limit(filters.limit),
            offset: filters.offset,
        };
        let jobs = JobsApi::new(self.repository.clone(), self.config.clone())
            .list_jobs(request_id.clone(), request)
            .map_err(|envelope| error_message(*envelope))?
            .clone();
        let meta = jobs.meta.pagination.clone();
        let items = jobs.data.unwrap_or_default();
        let selected_job = match filters.selected_job_id.as_deref() {
            Some(job_id) => Some(
                JobsApi::new(self.repository.clone(), self.config.clone())
                    .get_job(request_id, job_id)
                    .map_err(|envelope| error_message(*envelope))?
                    .data
                    .expect("job resource should exist"),
            ),
            None => None,
        };
        Ok(JobsScreen {
            summary: JobsScreenSummary {
                total: meta.map(|value| value.total).unwrap_or(items.len() as u64),
                running_count: items
                    .iter()
                    .filter(|job| job.status == JobStatusValue::Running)
                    .count(),
                failed_count: items
                    .iter()
                    .filter(|job| job.status == JobStatusValue::Failed)
                    .count(),
                resumable_count: items
                    .iter()
                    .filter(|job| job.status == JobStatusValue::Resumable)
                    .count(),
                selected_job_id: filters.selected_job_id.clone(),
            },
            filters,
            items,
            selected_job,
        })
    }

    pub fn retry(
        &self,
        request_id: impl Into<String>,
        job_id: &str,
        scope: RetryScopeValue,
        queued_at_unix_seconds: i64,
    ) -> Result<JobResource, String> {
        JobsApi::new(self.repository.clone(), self.config.clone())
            .retry_job(
                request_id,
                job_id,
                RetryJobRequest {
                    scope,
                    queued_at_unix_seconds,
                },
            )
            .map_err(|envelope| error_message(*envelope))?
            .data
            .ok_or_else(|| "job retry response was empty".to_string())
    }
}

fn normalize_limit(limit: u32) -> u32 {
    if limit == 0 { 50 } else { limit }
}

fn error_message<T>(envelope: crate::api::envelope::ApiEnvelope<T>) -> String {
    envelope
        .error
        .map(|error| error.message)
        .unwrap_or_else(|| "api request failed".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::api::issues::IssueStateValue;
    use crate::api::jobs::{JobStatusValue, RetryScopeValue};
    use crate::application::config::ValidatedRuntimeConfig;
    use crate::application::repository::{
        ExportRepository, ExportedMetadataListQuery, ImportBatchCommandRepository,
        ImportBatchListQuery, ImportBatchRepository, IssueCommandRepository, IssueListQuery,
        IssueRepository, JobCommandRepository, JobListQuery, JobRepository,
        ReleaseInstanceCommandRepository, ReleaseInstanceListQuery, ReleaseInstanceRepository,
        RepositoryError, SourceCommandRepository, SourceRepository,
        StagingManifestCommandRepository, StagingManifestRepository,
    };
    use crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot;
    use crate::domain::file::FileRole;
    use crate::domain::import_batch::ImportBatch;
    use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
    use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType};
    use crate::domain::release_instance::ReleaseInstance;
    use crate::domain::source::{Source, SourceLocator};
    use crate::domain::staging_manifest::StagingManifest;
    use crate::support::ids::{IssueId, JobId, ReleaseInstanceId};
    use crate::support::pagination::{Page, PageRequest};

    use super::*;

    #[test]
    fn issue_queue_loads_open_items_and_selected_detail() {
        let repository = InMemoryOperateRepository::seeded();
        let loader = IssueQueueScreenLoader::new(repository);

        let screen = loader
            .load(
                "req_issue_queue",
                IssueQueueFilters {
                    state: Some(IssueStateValue::Open),
                    selected_issue_id: Some("11111111-1111-1111-1111-111111111111".to_string()),
                    limit: 25,
                    offset: 0,
                    ..IssueQueueFilters::default()
                },
            )
            .expect("issue queue should load");

        assert_eq!(screen.items.len(), 2);
        assert_eq!(screen.summary.open_count, 2);
        assert_eq!(
            screen
                .selected_issue
                .expect("selected issue should exist")
                .export_diagnostics
                .expect("export diagnostics should exist")
                .export_profile,
            "generic_player"
        );
    }

    #[test]
    fn issue_queue_actions_resolve_and_suppress_items() {
        let repository = InMemoryOperateRepository::seeded();
        let loader = IssueQueueScreenLoader::new(repository.clone());

        let resolved = loader
            .act(
                "req_issue_resolve",
                IssueQueueActionRequest::Resolve {
                    issue_id: "11111111-1111-1111-1111-111111111111".to_string(),
                    resolved_at_unix_seconds: 200,
                },
            )
            .expect("resolve should succeed");
        assert_eq!(resolved.issue.state, IssueStateValue::Resolved);

        let suppressed = loader
            .act(
                "req_issue_suppress",
                IssueQueueActionRequest::Suppress {
                    issue_id: "22222222-2222-2222-2222-222222222222".to_string(),
                    reason: "operator reviewed".to_string(),
                    suppressed_at_unix_seconds: 250,
                },
            )
            .expect("suppress should succeed");
        assert_eq!(suppressed.issue.state, IssueStateValue::Suppressed);
        assert_eq!(
            suppressed.issue.suppressed_reason.as_deref(),
            Some("operator reviewed")
        );
    }

    #[test]
    fn jobs_screen_summarizes_running_and_failed_work() {
        let repository = InMemoryOperateRepository::seeded();
        let loader = JobsScreenLoader::new(
            repository,
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default()),
        );

        let screen = loader
            .load(
                "req_jobs",
                JobsScreenFilters {
                    selected_job_id: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string()),
                    limit: 25,
                    offset: 0,
                    ..JobsScreenFilters::default()
                },
            )
            .expect("jobs screen should load");

        assert_eq!(screen.summary.running_count, 1);
        assert_eq!(screen.summary.failed_count, 1);
        assert_eq!(
            screen
                .selected_job
                .expect("selected job should exist")
                .status,
            JobStatusValue::Running
        );
    }

    #[test]
    fn jobs_screen_retries_failed_jobs() {
        let repository = InMemoryOperateRepository::seeded();
        let loader = JobsScreenLoader::new(
            repository.clone(),
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default()),
        );

        let retried = loader
            .retry(
                "req_retry",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                RetryScopeValue::FullReprocess,
                300,
            )
            .expect("retry should succeed");

        assert_eq!(retried.status, JobStatusValue::Queued);
        assert_eq!(retried.retry_count, 2);
        assert_eq!(
            repository
                .jobs
                .lock()
                .expect("jobs should lock")
                .iter()
                .find(|job| job.id.as_uuid().to_string() == retried.id)
                .expect("retried job should persist")
                .status,
            JobStatus::Queued
        );
    }

    #[derive(Clone)]
    struct InMemoryOperateRepository {
        issues: Arc<Mutex<Vec<Issue>>>,
        jobs: Arc<Mutex<Vec<Job>>>,
        batches: Arc<Mutex<Vec<ImportBatch>>>,
        export_snapshot: ExportedMetadataSnapshot,
        release_instance: ReleaseInstance,
    }

    impl InMemoryOperateRepository {
        fn seeded() -> Self {
            let release_instance_id =
                ReleaseInstanceId::parse_str("33333333-3333-3333-3333-333333333333")
                    .expect("uuid should parse");
            let import_batch_id = crate::support::ids::ImportBatchId::parse_str(
                "44444444-4444-4444-4444-444444444444",
            )
            .expect("uuid should parse");
            let source_id =
                crate::support::ids::SourceId::parse_str("55555555-5555-5555-5555-555555555555")
                    .expect("uuid should parse");
            Self {
                issues: Arc::new(Mutex::new(vec![
                    Issue {
                        id: IssueId::parse_str("11111111-1111-1111-1111-111111111111")
                            .expect("uuid should parse"),
                        issue_type: IssueType::PlayerCompatibilityFailure,
                        state: IssueState::Open,
                        subject: IssueSubject::ReleaseInstance(release_instance_id.clone()),
                        summary: "Compatibility warning".to_string(),
                        details: Some("A player-visible field collision exists".to_string()),
                        created_at_unix_seconds: 100,
                        resolved_at_unix_seconds: None,
                        suppressed_reason: None,
                    },
                    Issue {
                        id: IssueId::parse_str("22222222-2222-2222-2222-222222222222")
                            .expect("uuid should parse"),
                        issue_type: IssueType::MissingArtwork,
                        state: IssueState::Open,
                        subject: IssueSubject::Library,
                        summary: "Artwork is missing".to_string(),
                        details: None,
                        created_at_unix_seconds: 110,
                        resolved_at_unix_seconds: None,
                        suppressed_reason: None,
                    },
                ])),
                jobs: Arc::new(Mutex::new(vec![
                    Job {
                        id: JobId::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                            .expect("uuid should parse"),
                        job_type: JobType::MatchReleaseInstance,
                        subject: JobSubject::ReleaseInstance(release_instance_id.clone()),
                        status: JobStatus::Running,
                        progress_phase: "matching".to_string(),
                        retry_count: 0,
                        triggered_by: JobTrigger::System,
                        created_at_unix_seconds: 100,
                        started_at_unix_seconds: Some(110),
                        finished_at_unix_seconds: None,
                        error_payload: None,
                    },
                    Job {
                        id: JobId::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                            .expect("uuid should parse"),
                        job_type: JobType::VerifyImport,
                        subject: JobSubject::ReleaseInstance(release_instance_id.clone()),
                        status: JobStatus::Failed,
                        progress_phase: "verifying".to_string(),
                        retry_count: 1,
                        triggered_by: JobTrigger::Operator,
                        created_at_unix_seconds: 120,
                        started_at_unix_seconds: Some(130),
                        finished_at_unix_seconds: Some(140),
                        error_payload: Some("verification failed".to_string()),
                    },
                ])),
                batches: Arc::new(Mutex::new(vec![ImportBatch {
                    id: import_batch_id.clone(),
                    source_id: source_id.clone(),
                    mode: crate::domain::import_batch::ImportMode::Copy,
                    status: crate::domain::import_batch::ImportBatchStatus::Grouped,
                    requested_by: crate::domain::import_batch::BatchRequester::Operator {
                        name: "operator".to_string(),
                    },
                    created_at_unix_seconds: 95,
                    received_paths: vec![std::path::PathBuf::from("/tmp/incoming")],
                }])),
                export_snapshot: ExportedMetadataSnapshot {
                    id: crate::support::ids::ExportedMetadataSnapshotId::new(),
                    release_instance_id: release_instance_id.clone(),
                    export_profile: "generic_player".to_string(),
                    album_title: "Kid A".to_string(),
                    album_artist: "Radiohead".to_string(),
                    artist_credits: vec!["Radiohead".to_string()],
                    edition_visibility:
                        crate::domain::exported_metadata_snapshot::QualifierVisibility::TagsAndPath,
                    technical_visibility:
                        crate::domain::exported_metadata_snapshot::QualifierVisibility::PathOnly,
                    path_components: vec!["Radiohead".to_string(), "Kid A [FLAC]".to_string()],
                    primary_artwork_filename: Some("cover.jpg".to_string()),
                    compatibility: crate::domain::exported_metadata_snapshot::CompatibilityReport {
                        verified: false,
                        warnings: vec!["album title collision".to_string()],
                    },
                    rendered_at_unix_seconds: 150,
                },
                release_instance: ReleaseInstance {
                    id: release_instance_id,
                    import_batch_id,
                    source_id,
                    release_id: None,
                    state: crate::domain::release_instance::ReleaseInstanceState::Failed,
                    technical_variant: crate::domain::release_instance::TechnicalVariant {
                        format_family: crate::domain::release_instance::FormatFamily::Flac,
                        bitrate_mode: crate::domain::release_instance::BitrateMode::Lossless,
                        bitrate_kbps: None,
                        sample_rate_hz: Some(44_100),
                        bit_depth: Some(16),
                        track_count: 10,
                        total_duration_seconds: 2900,
                    },
                    provenance: crate::domain::release_instance::ProvenanceSnapshot {
                        ingest_origin: crate::domain::release_instance::IngestOrigin::ManualAdd,
                        original_source_path: "/tmp/incoming".to_string(),
                        imported_at_unix_seconds: 90,
                        gazelle_reference: None,
                    },
                },
            }
        }
    }

    impl IssueRepository for InMemoryOperateRepository {
        fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .find(|issue| issue.id == *id)
                .cloned())
        }

        fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            let mut items = self.issues.lock().expect("issues should lock").clone();
            if let Some(state) = &query.state {
                items.retain(|issue| &issue.state == state);
            }
            if let Some(issue_type) = &query.issue_type {
                items.retain(|issue| &issue.issue_type == issue_type);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl IssueCommandRepository for InMemoryOperateRepository {
        fn create_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            unreachable!()
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            let mut issues = self.issues.lock().expect("issues should lock");
            let current = issues
                .iter_mut()
                .find(|current| current.id == issue.id)
                .expect("issue should exist");
            *current = issue.clone();
            Ok(())
        }
    }

    impl ExportRepository for InMemoryOperateRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(
                (self.export_snapshot.release_instance_id == *release_instance_id)
                    .then_some(self.export_snapshot.clone()),
            )
        }

        fn list_exported_metadata(
            &self,
            _query: &ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            unreachable!()
        }

        fn get_exported_metadata(
            &self,
            id: &crate::support::ids::ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok((self.export_snapshot.id == *id).then_some(self.export_snapshot.clone()))
        }
    }

    impl JobRepository for InMemoryOperateRepository {
        fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs should lock")
                .iter()
                .find(|job| job.id == *id)
                .cloned())
        }

        fn list_jobs(&self, query: &JobListQuery) -> Result<Page<Job>, RepositoryError> {
            let mut items = self.jobs.lock().expect("jobs should lock").clone();
            if let Some(status) = &query.status {
                items.retain(|job| &job.status == status);
            }
            if let Some(job_type) = &query.job_type {
                items.retain(|job| &job.job_type == job_type);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl JobCommandRepository for InMemoryOperateRepository {
        fn create_job(&self, job: &Job) -> Result<(), RepositoryError> {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .push(job.clone());
            Ok(())
        }

        fn update_job(&self, job: &Job) -> Result<(), RepositoryError> {
            let mut jobs = self.jobs.lock().expect("jobs should lock");
            let current = jobs
                .iter_mut()
                .find(|current| current.id == job.id)
                .expect("job should exist");
            *current = job.clone();
            Ok(())
        }

        fn list_recoverable_jobs(&self) -> Result<Vec<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs should lock")
                .iter()
                .filter(|job| matches!(job.status, JobStatus::Failed | JobStatus::Resumable))
                .cloned()
                .collect())
        }
    }

    impl ImportBatchCommandRepository for InMemoryOperateRepository {
        fn create_import_batch(&self, _batch: &ImportBatch) -> Result<(), RepositoryError> {
            unreachable!()
        }

        fn update_import_batch(&self, batch: &ImportBatch) -> Result<(), RepositoryError> {
            let mut batches = self.batches.lock().expect("batches should lock");
            let current = batches
                .iter_mut()
                .find(|current| current.id == batch.id)
                .expect("batch should exist");
            *current = batch.clone();
            Ok(())
        }

        fn list_active_import_batches_for_source(
            &self,
            _source_id: &crate::support::ids::SourceId,
        ) -> Result<Vec<ImportBatch>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ImportBatchRepository for InMemoryOperateRepository {
        fn get_import_batch(
            &self,
            id: &crate::support::ids::ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok(self
                .batches
                .lock()
                .expect("batches should lock")
                .iter()
                .find(|batch| batch.id == *id)
                .cloned())
        }

        fn list_import_batches(
            &self,
            _query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                total: 0,
                items: Vec::new(),
                request: PageRequest::default(),
            })
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryOperateRepository {
        fn create_release_instance(
            &self,
            _release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            unreachable!()
        }

        fn update_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            if self.release_instance.id == release_instance.id {
                Ok(())
            } else {
                Err(RepositoryError {
                    kind: crate::application::repository::RepositoryErrorKind::NotFound,
                    message: "release instance was not found".to_string(),
                })
            }
        }

        fn replace_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            unreachable!()
        }

        fn replace_candidate_matches_for_provider(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _provider: &crate::domain::candidate_match::CandidateProvider,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            unreachable!()
        }

        fn replace_track_instances_and_files(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _track_instances: &[crate::domain::track_instance::TrackInstance],
            _files: &[crate::domain::file::FileRecord],
        ) -> Result<(), RepositoryError> {
            unreachable!()
        }
    }

    impl ReleaseInstanceRepository for InMemoryOperateRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok((self.release_instance.id == *id).then_some(self.release_instance.clone()))
        }

        fn list_release_instances(
            &self,
            _query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            Ok(Page {
                total: 1,
                items: vec![self.release_instance.clone()],
                request: PageRequest::default(),
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(vec![self.release_instance.clone()])
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            unreachable!()
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            unreachable!()
        }

        fn list_track_instances_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<crate::domain::track_instance::TrackInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<FileRole>,
        ) -> Result<Vec<crate::domain::file::FileRecord>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl SourceCommandRepository for InMemoryOperateRepository {
        fn create_source(&self, _source: &Source) -> Result<(), RepositoryError> {
            unreachable!()
        }
    }

    impl SourceRepository for InMemoryOperateRepository {
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

    impl StagingManifestCommandRepository for InMemoryOperateRepository {
        fn create_staging_manifest(
            &self,
            _manifest: &StagingManifest,
        ) -> Result<(), RepositoryError> {
            unreachable!()
        }
    }

    impl StagingManifestRepository for InMemoryOperateRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &crate::support::ids::ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(Vec::new())
        }
    }
}
