use crate::application::config::ValidatedRuntimeConfig;
use crate::application::ingest::{
    WatchDiscoveryError, WatchDiscoveryReport, WatchDiscoveryService,
};
use crate::application::jobs::{JobService, JobServiceError};
use crate::application::repository::{
    ImportBatchCommandRepository, ImportBatchRepository, IssueCommandRepository, IssueRepository,
    JobCommandRepository, JobRepository, ReleaseInstanceCommandRepository,
    ReleaseInstanceRepository, RepositoryError, RepositoryErrorKind, SourceCommandRepository,
    SourceRepository, StagingManifestCommandRepository, StagingManifestRepository,
};
use crate::domain::import_batch::ImportBatchStatus;
use crate::domain::job::{Job, JobSubject, RetryScope};
use crate::domain::release_instance::ReleaseInstanceState;
use crate::support::ids::{JobId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryError {
    pub kind: RecoveryErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct RecoveryService<R> {
    repository: R,
    config: ValidatedRuntimeConfig,
}

impl<R> RecoveryService<R> {
    pub fn new(repository: R, config: ValidatedRuntimeConfig) -> Self {
        Self { repository, config }
    }
}

impl<R> RecoveryService<R>
where
    R: Clone
        + ImportBatchCommandRepository
        + ImportBatchRepository
        + IssueCommandRepository
        + IssueRepository
        + JobCommandRepository
        + JobRepository
        + ReleaseInstanceCommandRepository
        + ReleaseInstanceRepository
        + SourceCommandRepository
        + SourceRepository
        + StagingManifestCommandRepository
        + StagingManifestRepository,
{
    pub fn retry_job(
        &self,
        job_id: &JobId,
        scope: RetryScope,
        queued_at_unix_seconds: i64,
    ) -> Result<Job, RecoveryError> {
        let job = JobService::new(self.repository.clone())
            .retry_job(job_id, scope, queued_at_unix_seconds)
            .map_err(map_job_service_error)?;
        self.reset_subject_for_retry(&job.subject, scope)?;
        Ok(job)
    }

    pub fn reprocess_release_instance(
        &self,
        release_instance_id: &ReleaseInstanceId,
    ) -> Result<JobSubject, RecoveryError> {
        let mut release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| RecoveryError {
                kind: RecoveryErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        release_instance.state = ReleaseInstanceState::Staged;
        self.repository
            .update_release_instance(&release_instance)
            .map_err(map_repository_error)?;

        let mut batch = self
            .repository
            .get_import_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| RecoveryError {
                kind: RecoveryErrorKind::NotFound,
                message: format!(
                    "no import batch found for {}",
                    release_instance.import_batch_id.as_uuid()
                ),
            })?;
        batch.status = ImportBatchStatus::Created;
        self.repository
            .update_import_batch(&batch)
            .map_err(map_repository_error)?;

        Ok(JobSubject::ImportBatch(release_instance.import_batch_id))
    }

    pub fn rescan_watcher(
        &self,
        scan_subject: &str,
        discovered_at_unix_seconds: i64,
    ) -> Result<WatchDiscoveryReport, RecoveryError> {
        WatchDiscoveryService::new(self.repository.clone(), self.config.clone())
            .discover_watch_batches_for_scan(scan_subject, discovered_at_unix_seconds)
            .map_err(map_watch_discovery_error)
    }

    fn reset_subject_for_retry(
        &self,
        subject: &JobSubject,
        scope: RetryScope,
    ) -> Result<(), RecoveryError> {
        match subject {
            JobSubject::ReleaseInstance(release_instance_id) => {
                self.reset_release_instance_for_retry(release_instance_id, scope)
            }
            JobSubject::ImportBatch(batch_id) => {
                let release_instances = self
                    .repository
                    .list_release_instances_for_batch(batch_id)
                    .map_err(map_repository_error)?;
                for mut release_instance in release_instances {
                    release_instance.state = retry_state(scope);
                    self.repository
                        .update_release_instance(&release_instance)
                        .map_err(map_repository_error)?;
                }

                let mut batch = self
                    .repository
                    .get_import_batch(batch_id)
                    .map_err(map_repository_error)?
                    .ok_or_else(|| RecoveryError {
                        kind: RecoveryErrorKind::NotFound,
                        message: format!("no import batch found for {}", batch_id.as_uuid()),
                    })?;
                batch.status = retry_batch_status(scope);
                self.repository
                    .update_import_batch(&batch)
                    .map_err(map_repository_error)?;
                Ok(())
            }
            JobSubject::SourceScan(_) => Ok(()),
        }
    }

    fn reset_release_instance_for_retry(
        &self,
        release_instance_id: &ReleaseInstanceId,
        scope: RetryScope,
    ) -> Result<(), RecoveryError> {
        let mut release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| RecoveryError {
                kind: RecoveryErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;
        release_instance.state = retry_state(scope);
        let batch_id = release_instance.import_batch_id.clone();
        self.repository
            .update_release_instance(&release_instance)
            .map_err(map_repository_error)?;

        let mut batch = self
            .repository
            .get_import_batch(&batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| RecoveryError {
                kind: RecoveryErrorKind::NotFound,
                message: format!("no import batch found for {}", batch_id.as_uuid()),
            })?;
        batch.status = retry_batch_status(scope);
        self.repository
            .update_import_batch(&batch)
            .map_err(map_repository_error)?;
        Ok(())
    }
}

fn retry_state(scope: RetryScope) -> ReleaseInstanceState {
    match scope {
        RetryScope::Reanalyze | RetryScope::FullReprocess => ReleaseInstanceState::Staged,
        RetryScope::Rematch => ReleaseInstanceState::Analyzed,
        RetryScope::RerenderExport => ReleaseInstanceState::Matched,
        RetryScope::Retag => ReleaseInstanceState::RenderingExport,
        RetryScope::Reorganize => ReleaseInstanceState::Tagging,
    }
}

fn retry_batch_status(scope: RetryScope) -> ImportBatchStatus {
    match scope {
        RetryScope::Reanalyze | RetryScope::FullReprocess => ImportBatchStatus::Created,
        RetryScope::Rematch
        | RetryScope::RerenderExport
        | RetryScope::Retag
        | RetryScope::Reorganize => ImportBatchStatus::Grouped,
    }
}

fn map_repository_error(error: RepositoryError) -> RecoveryError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => RecoveryErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            RecoveryErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => RecoveryErrorKind::Storage,
    };
    RecoveryError {
        kind,
        message: error.message,
    }
}

fn map_job_service_error(error: JobServiceError) -> RecoveryError {
    let kind = match error.kind {
        crate::application::jobs::JobServiceErrorKind::NotFound => RecoveryErrorKind::NotFound,
        crate::application::jobs::JobServiceErrorKind::Conflict => RecoveryErrorKind::Conflict,
        crate::application::jobs::JobServiceErrorKind::Storage => RecoveryErrorKind::Storage,
    };
    RecoveryError {
        kind,
        message: error.message,
    }
}

fn map_watch_discovery_error(error: WatchDiscoveryError) -> RecoveryError {
    RecoveryError {
        kind: match error.kind {
            crate::application::ingest::WatchDiscoveryErrorKind::NotFound => {
                RecoveryErrorKind::NotFound
            }
            crate::application::ingest::WatchDiscoveryErrorKind::Conflict => {
                RecoveryErrorKind::Conflict
            }
            crate::application::ingest::WatchDiscoveryErrorKind::Storage => {
                RecoveryErrorKind::Storage
            }
            crate::application::ingest::WatchDiscoveryErrorKind::Io => RecoveryErrorKind::Storage,
        },
        message: error.message,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::application::repository::JobListQuery;
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
    use crate::domain::issue::Issue;
    use crate::domain::job::{JobStatus, JobTrigger, JobType};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        TechnicalVariant,
    };
    use crate::domain::source::{Source, SourceKind, SourceLocator};
    use crate::domain::staging_manifest::StagingManifest;
    use crate::domain::track_instance::TrackInstance;
    use crate::support::ids::{ImportBatchId, IssueId, SourceId};
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn retry_job_resets_release_instance_to_scope_state() {
        let repository = InMemoryRecoveryRepository::new();
        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = RecoveryService::new(repository.clone(), config);
        let job = repository.insert_job(Job::queued(
            JobType::WriteTags,
            JobSubject::ReleaseInstance(repository.release_instance_id.clone()),
            JobTrigger::Operator,
            100,
        ));

        JobService::new(repository.clone())
            .start_job(&job.id, "tagging", 101)
            .expect("job should start");
        JobService::new(repository.clone())
            .fail_job(&job.id, "tagging", "disk error", 102)
            .expect("job should fail");

        let retried = service
            .retry_job(&job.id, RetryScope::Retag, 103)
            .expect("retry should succeed");

        assert_eq!(retried.status, JobStatus::Queued);
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::RenderingExport
        );
        assert_eq!(repository.batch().status, ImportBatchStatus::Grouped);
    }

    #[test]
    fn reprocess_release_instance_resets_stage_inputs() {
        let repository = InMemoryRecoveryRepository::new();
        let config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        let service = RecoveryService::new(repository.clone(), config);

        let subject = service
            .reprocess_release_instance(&repository.release_instance_id)
            .expect("reprocess should succeed");

        assert_eq!(
            subject,
            JobSubject::ImportBatch(repository.batch_id.clone())
        );
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Staged
        );
        assert_eq!(repository.batch().status, ImportBatchStatus::Created);
    }

    #[derive(Clone)]
    struct InMemoryRecoveryRepository {
        batch_id: ImportBatchId,
        release_instance_id: ReleaseInstanceId,
        batches: Arc<Mutex<HashMap<String, ImportBatch>>>,
        release_instances: Arc<Mutex<HashMap<String, ReleaseInstance>>>,
        jobs: Arc<Mutex<HashMap<String, Job>>>,
    }

    impl InMemoryRecoveryRepository {
        fn new() -> Self {
            let source_id = SourceId::new();
            let batch_id = ImportBatchId::new();
            let release_instance_id = ReleaseInstanceId::new();
            let batch = ImportBatch {
                id: batch_id.clone(),
                source_id: source_id.clone(),
                mode: ImportMode::Copy,
                status: ImportBatchStatus::Quarantined,
                requested_by: BatchRequester::Operator {
                    name: "operator".to_string(),
                },
                created_at_unix_seconds: 1,
                received_paths: vec![],
            };
            let release_instance = ReleaseInstance {
                id: release_instance_id.clone(),
                import_batch_id: batch_id.clone(),
                source_id,
                release_id: None,
                state: ReleaseInstanceState::Failed,
                technical_variant: TechnicalVariant {
                    format_family: FormatFamily::Mp3,
                    bitrate_mode: BitrateMode::Variable,
                    bitrate_kbps: Some(320),
                    sample_rate_hz: Some(44_100),
                    bit_depth: None,
                    track_count: 1,
                    total_duration_seconds: 200,
                },
                provenance: ProvenanceSnapshot {
                    ingest_origin: IngestOrigin::ManualAdd,
                    original_source_path: "/tmp/input".to_string(),
                    imported_at_unix_seconds: 1,
                    gazelle_reference: None,
                },
            };

            let mut batches = HashMap::new();
            batches.insert(batch_id.as_uuid().to_string(), batch);
            let mut release_instances = HashMap::new();
            release_instances.insert(release_instance_id.as_uuid().to_string(), release_instance);

            Self {
                batch_id,
                release_instance_id,
                batches: Arc::new(Mutex::new(batches)),
                release_instances: Arc::new(Mutex::new(release_instances)),
                jobs: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn insert_job(&self, job: Job) -> Job {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            job
        }

        fn release_instance(&self) -> ReleaseInstance {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .get(&self.release_instance_id.as_uuid().to_string())
                .cloned()
                .expect("release instance should exist")
        }

        fn batch(&self) -> ImportBatch {
            self.batches
                .lock()
                .expect("batches should lock")
                .get(&self.batch_id.as_uuid().to_string())
                .cloned()
                .expect("batch should exist")
        }
    }

    impl JobRepository for InMemoryRecoveryRepository {
        fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("jobs should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_jobs(&self, query: &JobListQuery) -> Result<Page<Job>, RepositoryError> {
            let items = self
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
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl JobCommandRepository for InMemoryRecoveryRepository {
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

    impl ReleaseInstanceRepository for InMemoryRecoveryRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_release_instances(
            &self,
            _query: &crate::application::repository::ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: crate::support::pagination::PageRequest::new(50, 0),
                total: 0,
            })
        }

        fn list_release_instances_for_batch(
            &self,
            import_batch_id: &ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .values()
                .filter(|release_instance| &release_instance.import_batch_id == import_batch_id)
                .cloned()
                .collect())
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _page: &crate::support::pagination::PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: crate::support::pagination::PageRequest::new(50, 0),
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
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<TrackInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl ReleaseInstanceCommandRepository for InMemoryRecoveryRepository {
        fn create_release_instance(
            &self,
            _release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .insert(
                    release_instance.id.as_uuid().to_string(),
                    release_instance.clone(),
                );
            Ok(())
        }

        fn replace_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_candidate_matches_for_provider(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _provider: &crate::domain::candidate_match::CandidateProvider,
            _matches: &[crate::domain::candidate_match::CandidateMatch],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn replace_track_instances_and_files(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _track_instances: &[TrackInstance],
            _files: &[FileRecord],
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl ImportBatchRepository for InMemoryRecoveryRepository {
        fn get_import_batch(
            &self,
            id: &ImportBatchId,
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
            _query: &crate::application::repository::ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: crate::support::pagination::PageRequest::new(50, 0),
                total: 0,
            })
        }
    }

    impl ImportBatchCommandRepository for InMemoryRecoveryRepository {
        fn create_import_batch(&self, _batch: &ImportBatch) -> Result<(), RepositoryError> {
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
            _source_id: &SourceId,
        ) -> Result<Vec<ImportBatch>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl SourceRepository for InMemoryRecoveryRepository {
        fn get_source(&self, _id: &SourceId) -> Result<Option<Source>, RepositoryError> {
            Ok(Some(Source {
                id: SourceId::new(),
                kind: SourceKind::WatchDirectory,
                display_name: "watch".to_string(),
                locator: SourceLocator::FilesystemPath(std::path::PathBuf::from("/tmp/watch")),
                external_reference: None,
            }))
        }

        fn find_source_by_locator(
            &self,
            _locator: &SourceLocator,
        ) -> Result<Option<Source>, RepositoryError> {
            Ok(None)
        }
    }

    impl SourceCommandRepository for InMemoryRecoveryRepository {
        fn create_source(&self, _source: &Source) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl StagingManifestRepository for InMemoryRecoveryRepository {
        fn list_staging_manifests_for_batch(
            &self,
            _batch_id: &ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl StagingManifestCommandRepository for InMemoryRecoveryRepository {
        fn create_staging_manifest(
            &self,
            _manifest: &StagingManifest,
        ) -> Result<(), RepositoryError> {
            Ok(())
        }
    }

    impl IssueRepository for InMemoryRecoveryRepository {
        fn get_issue(&self, _id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(None)
        }

        fn list_issues(
            &self,
            _query: &crate::application::repository::IssueListQuery,
        ) -> Result<Page<Issue>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: crate::support::pagination::PageRequest::new(50, 0),
                total: 0,
            })
        }
    }

    impl IssueCommandRepository for InMemoryRecoveryRepository {
        fn create_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            Ok(())
        }

        fn update_issue(&self, _issue: &Issue) -> Result<(), RepositoryError> {
            Ok(())
        }
    }
}
