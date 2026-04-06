use crate::application::artwork::{ArtworkExportError, ArtworkService};
use crate::application::compatibility::{
    CompatibilityVerificationError, CompatibilityVerificationService,
};
use crate::application::config::ValidatedRuntimeConfig;
use crate::application::duplicates::{DuplicateHandlingError, DuplicateHandlingService};
use crate::application::export::{ExportRenderingError, ExportRenderingService};
use crate::application::ingest::{WatchDiscoveryError, WatchDiscoveryService};
use crate::application::issues::IssueServiceError;
use crate::application::jobs::{JobService, JobServiceError};
use crate::application::matching::{
    DiscogsMetadataProvider, MatchingServiceError, MusicBrainzMetadataProvider,
    ReleaseMatchingService,
};
use crate::application::observability::{LogLevel, ObservabilityContext, issue_type_name, labels};
use crate::application::organize::{FileOrganizationService, OrganizationError};
use crate::application::recovery::{RecoveryError, RecoveryService};
use crate::application::repository::{
    ExportCommandRepository, ExportRepository, ImportBatchCommandRepository, ImportBatchRepository,
    IngestEvidenceCommandRepository, IngestEvidenceRepository, IssueCommandRepository,
    IssueListQuery, IssueRepository, JobCommandRepository, JobListQuery, JobRepository,
    ManualOverrideCommandRepository, ManualOverrideRepository, MetadataSnapshotCommandRepository,
    MetadataSnapshotRepository, ReleaseArtworkCommandRepository, ReleaseArtworkRepository,
    ReleaseCommandRepository, ReleaseInstanceCommandRepository, ReleaseInstanceRepository,
    ReleaseRepository, RepositoryError, RepositoryErrorKind, SourceCommandRepository,
    SourceRepository, StagingManifestCommandRepository, StagingManifestRepository,
};
use crate::application::tagging::{TagWriterService, TaggingError, TaggingErrorKind};
use crate::application::workers::{WorkerPoolKind, WorkerPools};
use crate::domain::issue::{IssueState, IssueSubject, IssueType};
use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType};
use crate::domain::release_instance::ReleaseInstanceState;
use crate::support::ids::{ImportBatchId, JobId, ReleaseInstanceId};
use crate::support::pagination::PageRequest;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineRunReport {
    pub job: Job,
    pub queued_jobs: Vec<Job>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineError {
    pub kind: PipelineErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineErrorKind {
    NotFound,
    Conflict,
    Storage,
    Provider,
}

pub struct JobPipelineService<R, P> {
    repository: R,
    provider: P,
    config: ValidatedRuntimeConfig,
    observability: ObservabilityContext,
    workers: WorkerPools,
}

impl<R, P> JobPipelineService<R, P> {
    pub fn new(
        repository: R,
        provider: P,
        config: ValidatedRuntimeConfig,
        observability: ObservabilityContext,
        workers: WorkerPools,
    ) -> Self {
        Self {
            repository,
            provider,
            config,
            observability,
            workers,
        }
    }
}

impl<R, P> JobPipelineService<R, P>
where
    R: Clone
        + ExportCommandRepository
        + ExportRepository
        + ImportBatchCommandRepository
        + ImportBatchRepository
        + IngestEvidenceCommandRepository
        + IngestEvidenceRepository
        + IssueCommandRepository
        + IssueRepository
        + JobCommandRepository
        + JobRepository
        + ManualOverrideCommandRepository
        + ManualOverrideRepository
        + MetadataSnapshotCommandRepository
        + MetadataSnapshotRepository
        + ReleaseArtworkCommandRepository
        + ReleaseArtworkRepository
        + ReleaseCommandRepository
        + ReleaseInstanceCommandRepository
        + ReleaseInstanceRepository
        + ReleaseRepository
        + SourceCommandRepository
        + SourceRepository
        + StagingManifestCommandRepository
        + StagingManifestRepository,
    P: Clone + MusicBrainzMetadataProvider + DiscogsMetadataProvider,
{
    pub async fn run_job(
        &self,
        job_id: &JobId,
        changed_at_unix_seconds: i64,
    ) -> Result<PipelineRunReport, PipelineError> {
        let job = self.load_job(job_id)?;
        let started_at = Instant::now();
        self.emit_job_event(LogLevel::Info, "job_started", &job, Vec::new());
        let _permit = self
            .workers
            .pool(pool_kind_for_job(&job.job_type))
            .acquire()
            .await;
        let jobs = JobService::new(self.repository.clone());
        jobs.start_job(
            job_id,
            job.job_type.default_phase(),
            changed_at_unix_seconds,
        )
        .map_err(map_job_service_error)?;

        let result = match job.job_type {
            JobType::DiscoverBatch => self.run_discover_job(&job, changed_at_unix_seconds).await,
            JobType::AnalyzeReleaseInstance => {
                self.run_analyze_job(&job, changed_at_unix_seconds).await
            }
            JobType::MatchReleaseInstance => {
                self.run_match_job(&job, changed_at_unix_seconds).await
            }
            JobType::EnrichReleaseInstance => {
                self.run_enrich_job(&job, changed_at_unix_seconds).await
            }
            JobType::RenderExportMetadata => {
                self.run_render_job(&job, changed_at_unix_seconds).await
            }
            JobType::WriteTags => self.run_write_tags_job(&job, changed_at_unix_seconds).await,
            JobType::OrganizeFiles => self.run_organize_job(&job, changed_at_unix_seconds).await,
            JobType::VerifyImport => self.run_verify_job(&job, changed_at_unix_seconds).await,
            JobType::ReprocessReleaseInstance => {
                self.run_reprocess_job(&job, changed_at_unix_seconds).await
            }
            JobType::RescanWatcher => self.run_rescan_job(&job, changed_at_unix_seconds).await,
        };

        match result {
            Ok(queued_jobs) => {
                let phase = success_phase_for_job(&job.job_type, &queued_jobs);
                let completed = jobs
                    .complete_job(job_id, phase, changed_at_unix_seconds)
                    .map_err(map_job_service_error)?;
                self.record_job_completion(
                    &completed,
                    "succeeded",
                    started_at.elapsed().as_secs_f64(),
                );
                self.refresh_operational_gauges();
                Ok(PipelineRunReport {
                    job: completed,
                    queued_jobs,
                })
            }
            Err(error) => {
                self.handle_failure(&job, &error, changed_at_unix_seconds)
                    .await?;
                let failed = jobs
                    .fail_job(
                        job_id,
                        failure_phase_for_job(&job.job_type),
                        error.message.clone(),
                        changed_at_unix_seconds,
                    )
                    .map_err(map_job_service_error)?;
                self.record_job_completion(&failed, "failed", started_at.elapsed().as_secs_f64());
                self.refresh_operational_gauges();
                Err(PipelineError {
                    kind: map_pipeline_kind(&error.kind),
                    message: format!(
                        "job {} failed in phase {}: {}",
                        failed.id.as_uuid(),
                        failed.progress_phase,
                        error.message
                    ),
                })
            }
        }
    }

    async fn run_discover_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let batch_id = resolve_batch_subject(&self.repository, &job.subject)?;
        let mut batch = self
            .repository
            .get_import_batch(&batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                StageFailure::not_found(format!("no import batch found for {}", batch_id.as_uuid()))
            })?;
        batch.status = crate::domain::import_batch::ImportBatchStatus::Discovering;
        self.repository
            .update_import_batch(&batch)
            .map_err(map_repository_error)?;

        WatchDiscoveryService::new(self.repository.clone(), self.config.clone())
            .analyze_import_batch(&batch_id, changed_at_unix_seconds)
            .map_err(map_watch_error)?;
        self.observability
            .metrics
            .increment_counter("imports_total", labels([("outcome", "grouped")]));
        self.observability.emit(
            LogLevel::Info,
            "import_batch_grouped",
            [
                ("import_batch_id", batch_id.as_uuid().to_string()),
                ("job_id", job.id.as_uuid().to_string()),
            ],
        );

        let queued = self.enqueue_job_once(
            JobType::MatchReleaseInstance,
            JobSubject::ImportBatch(batch_id),
            JobTrigger::System,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_analyze_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let batch_id = resolve_batch_subject(&self.repository, &job.subject)?;
        WatchDiscoveryService::new(self.repository.clone(), self.config.clone())
            .analyze_import_batch(&batch_id, changed_at_unix_seconds)
            .map_err(map_watch_error)?;
        self.observability
            .metrics
            .increment_counter("imports_total", labels([("outcome", "grouped")]));
        let queued = self.enqueue_job_once(
            JobType::MatchReleaseInstance,
            JobSubject::ImportBatch(batch_id),
            JobTrigger::System,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_match_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let batch_id = resolve_batch_subject(&self.repository, &job.subject)?;
        let matcher = ReleaseMatchingService::new(self.repository.clone(), self.provider.clone());
        matcher
            .score_and_persist_batch_matches(&batch_id, changed_at_unix_seconds)
            .await
            .map_err(map_matching_error)?;
        let materialized = matcher
            .materialize_batch_matches(&batch_id)
            .await
            .map_err(map_matching_error)?;
        let mut queued = Vec::new();
        for group in materialized.groups {
            if let Some(job) = self.enqueue_job_once(
                JobType::EnrichReleaseInstance,
                JobSubject::ReleaseInstance(group.release_instance.id),
                JobTrigger::System,
                changed_at_unix_seconds,
            )? {
                queued.push(job);
            }
        }
        Ok(queued)
    }

    async fn run_enrich_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let release_instance_id = resolve_release_instance_subject(&job.subject)?;
        ReleaseMatchingService::new(self.repository.clone(), self.provider.clone())
            .enrich_release_instance_with_discogs(&release_instance_id, changed_at_unix_seconds)
            .await
            .map_err(map_matching_error)?;
        let duplicate_report = DuplicateHandlingService::new(self.repository.clone())
            .evaluate_release_instance(
                &self.config.import,
                &release_instance_id,
                changed_at_unix_seconds,
            )
            .map_err(map_duplicate_error)?;
        if !duplicate_report.duplicates.is_empty() {
            self.observability.metrics.increment_counter(
                "duplicate_detections_total",
                labels([(
                    "result",
                    if duplicate_report.quarantined {
                        "quarantined"
                    } else {
                        "flagged"
                    },
                )]),
            );
            self.observability.emit(
                LogLevel::Warn,
                "duplicate_detected",
                [
                    ("job_id", job.id.as_uuid().to_string()),
                    (
                        "release_instance_id",
                        release_instance_id.as_uuid().to_string(),
                    ),
                    ("duplicates", duplicate_report.duplicates.len().to_string()),
                ],
            );
        }
        if duplicate_report.quarantined {
            self.update_release_instance_state(
                &release_instance_id,
                ReleaseInstanceState::Quarantined,
            )?;
            self.update_import_batch_state_for_release_instance(
                &release_instance_id,
                crate::domain::import_batch::ImportBatchStatus::Quarantined,
            )?;
            return Ok(Vec::new());
        }
        let queued = self.enqueue_job_once(
            JobType::RenderExportMetadata,
            JobSubject::ReleaseInstance(release_instance_id),
            JobTrigger::System,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_render_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let release_instance_id = resolve_release_instance_subject(&job.subject)?;
        self.update_release_instance_state(
            &release_instance_id,
            ReleaseInstanceState::RenderingExport,
        )?;
        ExportRenderingService::new(self.repository.clone())
            .render_release_instance_snapshot(
                &self.config.export,
                &release_instance_id,
                changed_at_unix_seconds,
            )
            .map_err(map_export_error)?;
        let queued = self.enqueue_job_once(
            JobType::WriteTags,
            JobSubject::ReleaseInstance(release_instance_id),
            JobTrigger::System,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_write_tags_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let release_instance_id = resolve_release_instance_subject(&job.subject)?;
        self.update_release_instance_state(&release_instance_id, ReleaseInstanceState::Tagging)?;
        TagWriterService::new(self.repository.clone())
            .write_release_instance_tags(
                &self.config.export,
                &self.config.export.tagging,
                &release_instance_id,
            )
            .await
            .map_err(map_tagging_error)?;
        let queued = self.enqueue_job_once(
            JobType::OrganizeFiles,
            JobSubject::ReleaseInstance(release_instance_id),
            JobTrigger::System,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_organize_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let release_instance_id = resolve_release_instance_subject(&job.subject)?;
        self.update_release_instance_state(&release_instance_id, ReleaseInstanceState::Organizing)?;
        let report = FileOrganizationService::new(self.repository.clone())
            .organize_release_instance(
                &self.config.storage,
                &self.config.export,
                &release_instance_id,
            )
            .await
            .map_err(map_organization_error)?;
        self.observability.metrics.add_counter(
            "file_operations_total",
            labels([
                ("mode", import_mode_name(&report.mode)),
                ("result", "succeeded"),
            ]),
            report.organized_files.len() as f64,
        );
        self.observability.emit(
            LogLevel::Info,
            "managed_files_organized",
            [
                ("job_id", job.id.as_uuid().to_string()),
                (
                    "release_instance_id",
                    release_instance_id.as_uuid().to_string(),
                ),
                ("file_count", report.organized_files.len().to_string()),
                ("mode", import_mode_name(&report.mode).to_string()),
            ],
        );
        ArtworkService::new(self.repository.clone())
            .export_primary_artwork(
                &self.config.storage,
                &self.config.export,
                &release_instance_id,
                changed_at_unix_seconds,
            )
            .map_err(map_artwork_error)?;
        let queued = self.enqueue_job_once(
            JobType::VerifyImport,
            JobSubject::ReleaseInstance(release_instance_id),
            JobTrigger::System,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_verify_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let release_instance_id = resolve_release_instance_subject(&job.subject)?;
        let report = CompatibilityVerificationService::new(self.repository.clone())
            .verify_release_instance(&release_instance_id, changed_at_unix_seconds)
            .await
            .map_err(map_compatibility_error)?;
        if !report.verified {
            self.observability.metrics.add_counter(
                "compatibility_verification_failures_total",
                labels([("result", "failed")]),
                report.issue_types.len() as f64,
            );
            for issue_type in &report.issue_types {
                self.observability.emit(
                    LogLevel::Warn,
                    "compatibility_verification_failed",
                    [
                        ("job_id", job.id.as_uuid().to_string()),
                        (
                            "release_instance_id",
                            release_instance_id.as_uuid().to_string(),
                        ),
                        ("issue_type", issue_type_name(issue_type).to_string()),
                    ],
                );
            }
        }
        if report.verified {
            self.update_release_instance_state(
                &release_instance_id,
                ReleaseInstanceState::Verified,
            )?;
        }
        Ok(Vec::new())
    }

    async fn run_reprocess_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let release_instance_id = resolve_release_instance_subject(&job.subject)?;
        let subject = RecoveryService::new(self.repository.clone(), self.config.clone())
            .reprocess_release_instance(&release_instance_id)
            .map_err(map_recovery_error)?;
        let queued = self.enqueue_job_once(
            JobType::AnalyzeReleaseInstance,
            subject,
            JobTrigger::Operator,
            changed_at_unix_seconds,
        )?;
        Ok(queued.into_iter().collect())
    }

    async fn run_rescan_job(
        &self,
        job: &Job,
        changed_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, StageFailure> {
        let JobSubject::SourceScan(scan_subject) = &job.subject else {
            return Err(StageFailure::conflict(format!(
                "job type {} requires a source scan subject",
                job_type_name(&job.job_type)
            )));
        };
        let report = RecoveryService::new(self.repository.clone(), self.config.clone())
            .rescan_watcher(scan_subject, changed_at_unix_seconds)
            .map_err(map_recovery_error)?;
        self.observability.metrics.add_counter(
            "imports_total",
            labels([("outcome", "discovered")]),
            report.created_batches.len() as f64,
        );
        self.observability.metrics.add_counter(
            "imports_total",
            labels([("outcome", "skipped_active")]),
            report.skipped_paths.len() as f64,
        );
        self.observability.emit(
            LogLevel::Info,
            "watcher_rescanned",
            [
                ("job_id", job.id.as_uuid().to_string()),
                ("source_path", scan_subject.to_string()),
                ("created_batches", report.created_batches.len().to_string()),
                ("skipped_paths", report.skipped_paths.len().to_string()),
            ],
        );
        Ok(report.queued_jobs)
    }

    async fn handle_failure(
        &self,
        job: &Job,
        error: &StageFailure,
        changed_at_unix_seconds: i64,
    ) -> Result<(), PipelineError> {
        match (&job.job_type, &job.subject) {
            (JobType::WriteTags, JobSubject::ReleaseInstance(release_instance_id)) => {
                self.update_release_instance_state(
                    release_instance_id,
                    ReleaseInstanceState::Failed,
                )
                .map_err(|err| PipelineError {
                    kind: map_pipeline_kind(&err.kind),
                    message: err.message,
                })?;
                synchronize_issue(
                    &self.repository,
                    IssueType::BrokenTags,
                    IssueSubject::ReleaseInstance(release_instance_id.clone()),
                    format!("Tag writing failed for {}", release_instance_id.as_uuid()),
                    Some(error.message.clone()),
                    changed_at_unix_seconds,
                )
                .map_err(map_issue_service_error)?;
            }
            (JobType::OrganizeFiles, JobSubject::ReleaseInstance(release_instance_id)) => {
                self.observability.metrics.increment_counter(
                    "file_operations_total",
                    labels([("mode", "unknown"), ("result", "failed")]),
                );
                self.update_release_instance_state(
                    release_instance_id,
                    ReleaseInstanceState::Failed,
                )
                .map_err(|err| PipelineError {
                    kind: map_pipeline_kind(&err.kind),
                    message: err.message,
                })?;
                if error.kind == StageFailureKind::Conflict {
                    synchronize_issue(
                        &self.repository,
                        IssueType::UndistinguishableReleaseInstance,
                        IssueSubject::ReleaseInstance(release_instance_id.clone()),
                        format!(
                            "Managed output collision blocked {}",
                            release_instance_id.as_uuid()
                        ),
                        Some(error.message.clone()),
                        changed_at_unix_seconds,
                    )
                    .map_err(map_issue_service_error)?;
                }
            }
            _ => {}
        }
        self.emit_job_event(
            LogLevel::Error,
            "job_stage_failed",
            job,
            vec![("message".to_string(), error.message.clone())],
        );
        Ok(())
    }

    fn load_job(&self, job_id: &JobId) -> Result<Job, PipelineError> {
        self.repository
            .get_job(job_id)
            .map_err(map_pipeline_repository_error)?
            .ok_or_else(|| PipelineError {
                kind: PipelineErrorKind::NotFound,
                message: format!("job {} was not found", job_id.as_uuid()),
            })
    }

    fn record_job_completion(&self, job: &Job, status: &str, duration_seconds: f64) {
        self.observability.metrics.increment_counter(
            "jobs_total",
            labels([("type", job_type_name(&job.job_type)), ("status", status)]),
        );
        self.observability.metrics.observe_duration_seconds(
            "job_duration_seconds",
            labels([("type", job_type_name(&job.job_type))]),
            duration_seconds,
        );
        self.emit_job_event(
            if status == "succeeded" {
                LogLevel::Info
            } else {
                LogLevel::Error
            },
            if status == "succeeded" {
                "job_completed"
            } else {
                "job_failed"
            },
            job,
            vec![("status".to_string(), status.to_string())],
        );
    }

    fn emit_job_event(
        &self,
        level: LogLevel,
        event: &str,
        job: &Job,
        extra_fields: Vec<(String, String)>,
    ) {
        let mut fields = vec![
            ("job_id".to_string(), job.id.as_uuid().to_string()),
            (
                "job_type".to_string(),
                job_type_name(&job.job_type).to_string(),
            ),
        ];
        match &job.subject {
            JobSubject::ImportBatch(batch_id) => {
                fields.push((
                    "import_batch_id".to_string(),
                    batch_id.as_uuid().to_string(),
                ));
            }
            JobSubject::ReleaseInstance(release_instance_id) => {
                fields.push((
                    "release_instance_id".to_string(),
                    release_instance_id.as_uuid().to_string(),
                ));
            }
            JobSubject::SourceScan(scan_subject) => {
                fields.push(("source_path".to_string(), scan_subject.clone()));
            }
        }
        fields.extend(extra_fields);
        self.observability.emit(level, event, fields);
    }

    fn refresh_operational_gauges(&self) {
        self.observability.sync_issue_gauges(&self.repository);
        self.observability
            .sync_release_instance_state_gauges(&self.repository);
    }

    fn update_release_instance_state(
        &self,
        release_instance_id: &ReleaseInstanceId,
        state: ReleaseInstanceState,
    ) -> Result<(), StageFailure> {
        let mut release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                StageFailure::not_found(format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ))
            })?;
        release_instance.state = state;
        self.repository
            .update_release_instance(&release_instance)
            .map_err(map_repository_error)
    }

    fn update_import_batch_state_for_release_instance(
        &self,
        release_instance_id: &ReleaseInstanceId,
        status: crate::domain::import_batch::ImportBatchStatus,
    ) -> Result<(), StageFailure> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                StageFailure::not_found(format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ))
            })?;
        let mut batch = self
            .repository
            .get_import_batch(&release_instance.import_batch_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| {
                StageFailure::not_found(format!(
                    "no import batch found for {}",
                    release_instance.import_batch_id.as_uuid()
                ))
            })?;
        batch.status = status;
        self.repository
            .update_import_batch(&batch)
            .map_err(map_repository_error)
    }

    fn enqueue_job_once(
        &self,
        job_type: JobType,
        subject: JobSubject,
        triggered_by: JobTrigger,
        created_at_unix_seconds: i64,
    ) -> Result<Option<Job>, StageFailure> {
        if self.has_active_job(&job_type, &subject)? {
            return Ok(None);
        }
        JobService::new(self.repository.clone())
            .enqueue_job(job_type, subject, triggered_by, created_at_unix_seconds)
            .map(Some)
            .map_err(map_job_service_error_to_stage)
    }

    fn has_active_job(
        &self,
        job_type: &JobType,
        subject: &JobSubject,
    ) -> Result<bool, StageFailure> {
        for status in [JobStatus::Queued, JobStatus::Running, JobStatus::Resumable] {
            let jobs = self
                .repository
                .list_jobs(&JobListQuery {
                    status: Some(status),
                    job_type: Some(job_type.clone()),
                    page: PageRequest::new(100, 0),
                })
                .map_err(map_repository_error)?;
            if jobs.items.iter().any(|job| &job.subject == subject) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StageFailure {
    kind: StageFailureKind,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StageFailureKind {
    NotFound,
    Conflict,
    Storage,
    Provider,
}

impl StageFailure {
    fn not_found(message: String) -> Self {
        Self {
            kind: StageFailureKind::NotFound,
            message,
        }
    }

    fn conflict(message: String) -> Self {
        Self {
            kind: StageFailureKind::Conflict,
            message,
        }
    }

    fn storage(message: String) -> Self {
        Self {
            kind: StageFailureKind::Storage,
            message,
        }
    }
}

fn resolve_batch_subject<R>(
    repository: &R,
    subject: &JobSubject,
) -> Result<ImportBatchId, StageFailure>
where
    R: ReleaseInstanceRepository,
{
    match subject {
        JobSubject::ImportBatch(batch_id) => Ok(batch_id.clone()),
        JobSubject::ReleaseInstance(release_instance_id) => repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .map(|release_instance| release_instance.import_batch_id)
            .ok_or_else(|| {
                StageFailure::not_found(format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ))
            }),
        JobSubject::SourceScan(name) => Err(StageFailure {
            kind: StageFailureKind::Conflict,
            message: format!("source-scan subject {name} cannot resolve an import batch"),
        }),
    }
}

fn resolve_release_instance_subject(
    subject: &JobSubject,
) -> Result<ReleaseInstanceId, StageFailure> {
    match subject {
        JobSubject::ReleaseInstance(release_instance_id) => Ok(release_instance_id.clone()),
        JobSubject::ImportBatch(batch_id) => Err(StageFailure {
            kind: StageFailureKind::Conflict,
            message: format!(
                "import batch {} cannot resolve a release instance",
                batch_id.as_uuid()
            ),
        }),
        JobSubject::SourceScan(name) => Err(StageFailure {
            kind: StageFailureKind::Conflict,
            message: format!("source-scan subject {name} cannot resolve a release instance"),
        }),
    }
}

fn synchronize_issue<R>(
    repository: &R,
    issue_type: IssueType,
    subject: IssueSubject,
    summary: String,
    details: Option<String>,
    changed_at_unix_seconds: i64,
) -> Result<(), IssueServiceError>
where
    R: IssueRepository + IssueCommandRepository,
{
    let existing = repository
        .list_issues(&IssueListQuery {
            state: Some(IssueState::Open),
            issue_type: Some(issue_type.clone()),
            subject: Some(subject.clone()),
            page: PageRequest::new(20, 0),
        })
        .map_err(map_issue_repository_error)?;
    if existing.items.is_empty() {
        repository
            .create_issue(&crate::domain::issue::Issue::open(
                issue_type,
                subject,
                summary,
                details,
                changed_at_unix_seconds,
            ))
            .map_err(map_issue_repository_error)?;
    }
    Ok(())
}

fn map_issue_repository_error(error: RepositoryError) -> IssueServiceError {
    IssueServiceError {
        kind: match error.kind {
            RepositoryErrorKind::NotFound => {
                crate::application::issues::IssueServiceErrorKind::NotFound
            }
            RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                crate::application::issues::IssueServiceErrorKind::Conflict
            }
            RepositoryErrorKind::Storage => {
                crate::application::issues::IssueServiceErrorKind::Storage
            }
        },
        message: error.message,
    }
}

fn success_phase_for_job(job_type: &JobType, queued_jobs: &[Job]) -> &'static str {
    match job_type {
        JobType::DiscoverBatch | JobType::AnalyzeReleaseInstance => "grouped",
        JobType::MatchReleaseInstance if queued_jobs.is_empty() => "review_required",
        JobType::MatchReleaseInstance => "matched",
        JobType::EnrichReleaseInstance => "enriched",
        JobType::RenderExportMetadata => "rendered_export",
        JobType::WriteTags => "wrote_tags",
        JobType::OrganizeFiles => "organized",
        JobType::VerifyImport => "verified",
        JobType::ReprocessReleaseInstance => "reprocessed",
        JobType::RescanWatcher => "rescanned",
    }
}

fn failure_phase_for_job(job_type: &JobType) -> &'static str {
    match job_type {
        JobType::DiscoverBatch => "discovering",
        JobType::AnalyzeReleaseInstance => "analyzing",
        JobType::MatchReleaseInstance => "matching",
        JobType::EnrichReleaseInstance => "enriching",
        JobType::RenderExportMetadata => "rendering_export",
        JobType::WriteTags => "tagging",
        JobType::OrganizeFiles => "organizing",
        JobType::VerifyImport => "verifying",
        JobType::ReprocessReleaseInstance => "reprocessing",
        JobType::RescanWatcher => "rescanning",
    }
}

fn pool_kind_for_job(job_type: &JobType) -> WorkerPoolKind {
    match job_type {
        JobType::DiscoverBatch
        | JobType::AnalyzeReleaseInstance
        | JobType::WriteTags
        | JobType::OrganizeFiles
        | JobType::VerifyImport
        | JobType::RescanWatcher => WorkerPoolKind::FileIo,
        JobType::MatchReleaseInstance | JobType::EnrichReleaseInstance => {
            WorkerPoolKind::ProviderRequests
        }
        JobType::RenderExportMetadata | JobType::ReprocessReleaseInstance => {
            WorkerPoolKind::DbWrites
        }
    }
}

fn job_type_name(job_type: &JobType) -> &'static str {
    match job_type {
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
}

trait JobPhaseDefaults {
    fn default_phase(&self) -> &'static str;
}

impl JobPhaseDefaults for JobType {
    fn default_phase(&self) -> &'static str {
        failure_phase_for_job(self)
    }
}

fn map_repository_error(error: RepositoryError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            RepositoryErrorKind::NotFound => StageFailureKind::NotFound,
            RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                StageFailureKind::Conflict
            }
            RepositoryErrorKind::Storage => StageFailureKind::Storage,
        },
        message: error.message,
    }
}

fn map_watch_error(error: WatchDiscoveryError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::ingest::WatchDiscoveryErrorKind::NotFound => {
                StageFailureKind::NotFound
            }
            crate::application::ingest::WatchDiscoveryErrorKind::Conflict => {
                StageFailureKind::Conflict
            }
            crate::application::ingest::WatchDiscoveryErrorKind::Storage
            | crate::application::ingest::WatchDiscoveryErrorKind::Io => StageFailureKind::Storage,
        },
        message: error.message,
    }
}

fn map_matching_error(error: MatchingServiceError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::matching::MatchingServiceErrorKind::NotFound => {
                StageFailureKind::NotFound
            }
            crate::application::matching::MatchingServiceErrorKind::Conflict => {
                StageFailureKind::Conflict
            }
            crate::application::matching::MatchingServiceErrorKind::Storage => {
                StageFailureKind::Storage
            }
            crate::application::matching::MatchingServiceErrorKind::Provider => {
                StageFailureKind::Provider
            }
        },
        message: error.message,
    }
}

fn map_duplicate_error(error: DuplicateHandlingError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::duplicates::DuplicateHandlingErrorKind::NotFound => {
                StageFailureKind::NotFound
            }
            crate::application::duplicates::DuplicateHandlingErrorKind::Conflict => {
                StageFailureKind::Conflict
            }
            crate::application::duplicates::DuplicateHandlingErrorKind::Storage => {
                StageFailureKind::Storage
            }
        },
        message: error.message,
    }
}

fn map_export_error(error: ExportRenderingError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::export::ExportRenderingErrorKind::NotFound => {
                StageFailureKind::NotFound
            }
            crate::application::export::ExportRenderingErrorKind::Conflict => {
                StageFailureKind::Conflict
            }
            crate::application::export::ExportRenderingErrorKind::Storage => {
                StageFailureKind::Storage
            }
        },
        message: error.message,
    }
}

fn map_tagging_error(error: TaggingError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            TaggingErrorKind::NotFound => StageFailureKind::NotFound,
            TaggingErrorKind::Conflict | TaggingErrorKind::Unsupported => {
                StageFailureKind::Conflict
            }
            TaggingErrorKind::Storage => StageFailureKind::Storage,
        },
        message: error.message,
    }
}

fn map_organization_error(error: OrganizationError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::organize::OrganizationErrorKind::NotFound => {
                StageFailureKind::NotFound
            }
            crate::application::organize::OrganizationErrorKind::Conflict => {
                StageFailureKind::Conflict
            }
            crate::application::organize::OrganizationErrorKind::Storage => {
                StageFailureKind::Storage
            }
        },
        message: error.message,
    }
}

fn map_artwork_error(error: ArtworkExportError) -> StageFailure {
    match error.kind {
        crate::application::artwork::ArtworkExportErrorKind::NotFound => {
            StageFailure::not_found(error.message)
        }
        crate::application::artwork::ArtworkExportErrorKind::Conflict => {
            StageFailure::conflict(error.message)
        }
        crate::application::artwork::ArtworkExportErrorKind::Storage => {
            StageFailure::storage(error.message)
        }
    }
}

fn map_compatibility_error(error: CompatibilityVerificationError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::compatibility::CompatibilityVerificationErrorKind::NotFound => {
                StageFailureKind::NotFound
            }
            crate::application::compatibility::CompatibilityVerificationErrorKind::Conflict => {
                StageFailureKind::Conflict
            }
            crate::application::compatibility::CompatibilityVerificationErrorKind::Storage => {
                StageFailureKind::Storage
            }
        },
        message: error.message,
    }
}

fn import_mode_name(value: &crate::domain::import_batch::ImportMode) -> &'static str {
    match value {
        crate::domain::import_batch::ImportMode::Copy => "copy",
        crate::domain::import_batch::ImportMode::Move => "move",
        crate::domain::import_batch::ImportMode::Hardlink => "hardlink",
    }
}

fn map_recovery_error(error: RecoveryError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::recovery::RecoveryErrorKind::NotFound => StageFailureKind::NotFound,
            crate::application::recovery::RecoveryErrorKind::Conflict => StageFailureKind::Conflict,
            crate::application::recovery::RecoveryErrorKind::Storage => StageFailureKind::Storage,
        },
        message: error.message,
    }
}

fn map_job_service_error(error: JobServiceError) -> PipelineError {
    PipelineError {
        kind: match error.kind {
            crate::application::jobs::JobServiceErrorKind::NotFound => PipelineErrorKind::NotFound,
            crate::application::jobs::JobServiceErrorKind::Conflict => PipelineErrorKind::Conflict,
            crate::application::jobs::JobServiceErrorKind::Storage => PipelineErrorKind::Storage,
        },
        message: error.message,
    }
}

fn map_job_service_error_to_stage(error: JobServiceError) -> StageFailure {
    StageFailure {
        kind: match error.kind {
            crate::application::jobs::JobServiceErrorKind::NotFound => StageFailureKind::NotFound,
            crate::application::jobs::JobServiceErrorKind::Conflict => StageFailureKind::Conflict,
            crate::application::jobs::JobServiceErrorKind::Storage => StageFailureKind::Storage,
        },
        message: error.message,
    }
}

fn map_issue_service_error(error: IssueServiceError) -> PipelineError {
    PipelineError {
        kind: match error.kind {
            crate::application::issues::IssueServiceErrorKind::NotFound => {
                PipelineErrorKind::NotFound
            }
            crate::application::issues::IssueServiceErrorKind::Conflict => {
                PipelineErrorKind::Conflict
            }
            crate::application::issues::IssueServiceErrorKind::Storage => {
                PipelineErrorKind::Storage
            }
        },
        message: error.message,
    }
}

fn map_pipeline_repository_error(error: RepositoryError) -> PipelineError {
    PipelineError {
        kind: match error.kind {
            RepositoryErrorKind::NotFound => PipelineErrorKind::NotFound,
            RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                PipelineErrorKind::Conflict
            }
            RepositoryErrorKind::Storage => PipelineErrorKind::Storage,
        },
        message: error.message,
    }
}

fn map_pipeline_kind(kind: &StageFailureKind) -> PipelineErrorKind {
    match kind {
        StageFailureKind::NotFound => PipelineErrorKind::NotFound,
        StageFailureKind::Conflict => PipelineErrorKind::Conflict,
        StageFailureKind::Storage => PipelineErrorKind::Storage,
        StageFailureKind::Provider => PipelineErrorKind::Provider,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use id3::TagLike;

    use super::*;
    use crate::application::matching::{
        DiscogsReleaseCandidate, DiscogsReleaseQuery, MusicBrainzArtistCredit,
        MusicBrainzLabelInfo, MusicBrainzReleaseCandidate, MusicBrainzReleaseDetail,
        MusicBrainzReleaseGroupCandidate, MusicBrainzReleaseGroupRef,
    };
    use crate::application::repository::{
        ExportedMetadataListQuery, ImportBatchListQuery, JobListQuery, ManualOverrideListQuery,
        ReleaseGroupSearchQuery, ReleaseInstanceListQuery, ReleaseListQuery,
    };
    use crate::config::AppConfig;
    use crate::domain::artist::Artist;
    use crate::domain::candidate_match::{CandidateMatch, CandidateProvider};
    use crate::domain::exported_metadata_snapshot::{
        CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
    };
    use crate::domain::file::{FileRecord, FileRole};
    use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
    use crate::domain::ingest_evidence::IngestEvidenceRecord;
    use crate::domain::issue::Issue;
    use crate::domain::manual_override::ManualOverride;
    use crate::domain::metadata_snapshot::{MetadataSnapshot, MetadataSubject};
    use crate::domain::release::{PartialDate, Release, ReleaseEdition};
    use crate::domain::release_artwork::ReleaseArtwork;
    use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        TechnicalVariant,
    };
    use crate::domain::source::{Source, SourceKind, SourceLocator};
    use crate::domain::staging_manifest::{
        AuxiliaryFile, AuxiliaryFileRole, GroupingDecision, GroupingStrategy, StagedReleaseGroup,
        StagingManifest, StagingManifestSource,
    };
    use crate::domain::track::{Track, TrackPosition};
    use crate::domain::track_instance::TrackInstance;
    use crate::support::ids::{
        ArtistId, ExportedMetadataSnapshotId, ImportBatchId, JobId, ManualOverrideId,
        MusicBrainzReleaseGroupId, MusicBrainzReleaseId, MusicBrainzTrackId, ReleaseGroupId,
        ReleaseId, ReleaseInstanceId, SourceId, StagingManifestId, TrackId,
    };
    use crate::support::pagination::{Page, PageRequest};

    #[tokio::test(flavor = "current_thread")]
    async fn discover_and_match_jobs_queue_next_stages() {
        let root = test_root("pipeline-discover-match");
        let album_dir = root.join("incoming/Kid A");
        fs::create_dir_all(&album_dir).expect("album dir should exist");
        let mp3_path = album_dir.join("01 - Everything in Its Right Place.mp3");
        seed_mp3(
            &mp3_path,
            "Radiohead",
            "Kid A",
            "Everything in Its Right Place",
        );

        let repository =
            InMemoryPipelineRepository::for_batch_pipeline(&album_dir, vec![mp3_path.clone()]);
        let discover_job = repository.insert_job(Job::queued(
            JobType::DiscoverBatch,
            JobSubject::ImportBatch(repository.batch_id()),
            JobTrigger::System,
            10,
        ));
        let service = JobPipelineService::new(
            repository.clone(),
            FakePipelineProvider::default(),
            ValidatedRuntimeConfig::from_validated_app_config(&AppConfig::default()),
            crate::application::observability::ObservabilityContext::default(),
            WorkerPools::from_config(&AppConfig::default().workers),
        );

        let discover = service
            .run_job(&discover_job.id, 20)
            .await
            .expect("discover job should succeed");
        assert_eq!(discover.job.status, JobStatus::Succeeded);
        assert_eq!(discover.job.progress_phase, "grouped");
        assert_eq!(discover.queued_jobs.len(), 1);
        assert_eq!(
            discover.queued_jobs[0].job_type,
            JobType::MatchReleaseInstance
        );
        assert_eq!(repository.batch_status(), ImportBatchStatus::Grouped);

        let matched = service
            .run_job(&discover.queued_jobs[0].id, 30)
            .await
            .expect("match job should succeed");
        assert_eq!(matched.job.status, JobStatus::Succeeded);
        assert_eq!(matched.queued_jobs.len(), 1);
        assert_eq!(
            matched.queued_jobs[0].job_type,
            JobType::EnrichReleaseInstance
        );
        let release_instances = repository.release_instances();
        assert_eq!(release_instances.len(), 1);
        assert_eq!(release_instances[0].state, ReleaseInstanceState::Matched);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn render_tag_organize_and_verify_jobs_reach_verified() {
        let root = test_root("pipeline-import");
        let source_dir = root.join("incoming/Kid A");
        fs::create_dir_all(&source_dir).expect("source dir should exist");
        let mp3_path = source_dir.join("01 - Everything in Its Right Place.mp3");
        seed_mp3(
            &mp3_path,
            "Radiohead",
            "Kid A",
            "Everything in Its Right Place",
        );

        let repository = InMemoryPipelineRepository::for_release_instance_pipeline(
            &source_dir,
            mp3_path.clone(),
        );
        let mut config = AppConfig::default();
        config.storage.managed_library_root = root.join("managed");
        let validated = ValidatedRuntimeConfig::from_validated_app_config(&config);
        let service = JobPipelineService::new(
            repository.clone(),
            FakePipelineProvider::default(),
            validated,
            crate::application::observability::ObservabilityContext::default(),
            WorkerPools::from_config(&config.workers),
        );
        let render_job = repository.insert_job(Job::queued(
            JobType::RenderExportMetadata,
            JobSubject::ReleaseInstance(repository.release_instance_id()),
            JobTrigger::System,
            10,
        ));

        let rendered = service
            .run_job(&render_job.id, 20)
            .await
            .expect("render job should succeed");
        assert_eq!(rendered.queued_jobs[0].job_type, JobType::WriteTags);
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::RenderingExport
        );

        let tagged = service
            .run_job(&rendered.queued_jobs[0].id, 30)
            .await
            .expect("write-tags job should succeed");
        assert_eq!(tagged.queued_jobs[0].job_type, JobType::OrganizeFiles);
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Tagging
        );

        let organized = service
            .run_job(&tagged.queued_jobs[0].id, 40)
            .await
            .expect("organize job should succeed");
        assert_eq!(organized.queued_jobs[0].job_type, JobType::VerifyImport);
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Imported
        );
        let managed_file = repository
            .managed_files(repository.release_instance_id())
            .pop()
            .expect("managed file should exist");
        assert!(
            managed_file
                .path
                .parent()
                .expect("managed parent should exist")
                .join("cover.jpg")
                .is_file()
        );

        let verified = service
            .run_job(&organized.queued_jobs[0].id, 50)
            .await
            .expect("verify job should succeed");
        assert_eq!(verified.job.progress_phase, "verified");
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Verified
        );
        assert!(repository.open_issues().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn write_tags_failure_opens_broken_tags_issue() {
        let root = test_root("pipeline-tag-failure");
        let source_dir = root.join("incoming/Kid A");
        fs::create_dir_all(&source_dir).expect("source dir should exist");
        let missing_path = source_dir.join("01 - Missing.mp3");

        let repository =
            InMemoryPipelineRepository::for_release_instance_pipeline(&source_dir, missing_path);
        repository.seed_export_snapshot();
        let service = JobPipelineService::new(
            repository.clone(),
            FakePipelineProvider::default(),
            ValidatedRuntimeConfig::from_validated_app_config(&AppConfig::default()),
            crate::application::observability::ObservabilityContext::default(),
            WorkerPools::from_config(&AppConfig::default().workers),
        );
        let write_job = repository.insert_job(Job::queued(
            JobType::WriteTags,
            JobSubject::ReleaseInstance(repository.release_instance_id()),
            JobTrigger::System,
            10,
        ));

        let error = service
            .run_job(&write_job.id, 20)
            .await
            .expect_err("write-tags job should fail");
        assert_eq!(error.kind, PipelineErrorKind::Storage);
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Failed
        );
        assert!(
            repository
                .open_issues()
                .iter()
                .any(|issue| issue.issue_type == IssueType::BrokenTags)
        );
        assert_eq!(repository.job(&write_job.id).status, JobStatus::Failed);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enrich_job_quarantines_duplicates_when_configured() {
        let root = test_root("pipeline-duplicate-quarantine");
        let source_dir = root.join("incoming/Kid A");
        fs::create_dir_all(&source_dir).expect("source dir should exist");
        let mp3_path = source_dir.join("01 - Everything in Its Right Place.mp3");
        seed_mp3(
            &mp3_path,
            "Radiohead",
            "Kid A",
            "Everything in Its Right Place",
        );

        let repository = InMemoryPipelineRepository::for_release_instance_pipeline(
            &source_dir,
            mp3_path.clone(),
        );
        repository.seed_duplicate_release_instance();
        let mut config = AppConfig::default();
        config.import.duplicate_policy = crate::config::DuplicatePolicy::Quarantine;
        let service = JobPipelineService::new(
            repository.clone(),
            FakePipelineProvider::default(),
            ValidatedRuntimeConfig::from_validated_app_config(&config),
            crate::application::observability::ObservabilityContext::default(),
            WorkerPools::from_config(&config.workers),
        );
        let enrich_job = repository.insert_job(Job::queued(
            JobType::EnrichReleaseInstance,
            JobSubject::ReleaseInstance(repository.release_instance_id()),
            JobTrigger::System,
            10,
        ));

        let report = service
            .run_job(&enrich_job.id, 20)
            .await
            .expect("enrich job should succeed");

        assert_eq!(report.queued_jobs.len(), 0);
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Quarantined
        );
        assert_eq!(repository.batch_status(), ImportBatchStatus::Quarantined);
        assert!(
            repository
                .open_issues()
                .iter()
                .any(|issue| { issue.issue_type == IssueType::DuplicateReleaseInstance })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reprocess_job_queues_batch_analysis() {
        let root = test_root("pipeline-reprocess");
        let source_dir = root.join("incoming/Kid A");
        fs::create_dir_all(&source_dir).expect("source dir should exist");
        let mp3_path = source_dir.join("01 - Everything in Its Right Place.mp3");
        seed_mp3(
            &mp3_path,
            "Radiohead",
            "Kid A",
            "Everything in Its Right Place",
        );

        let repository = InMemoryPipelineRepository::for_release_instance_pipeline(
            &source_dir,
            mp3_path.clone(),
        );
        let config = AppConfig::default();
        let service = JobPipelineService::new(
            repository.clone(),
            FakePipelineProvider::default(),
            ValidatedRuntimeConfig::from_validated_app_config(&config),
            crate::application::observability::ObservabilityContext::default(),
            WorkerPools::from_config(&config.workers),
        );
        let reprocess_job = repository.insert_job(Job::queued(
            JobType::ReprocessReleaseInstance,
            JobSubject::ReleaseInstance(repository.release_instance_id()),
            JobTrigger::Operator,
            10,
        ));

        let report = service
            .run_job(&reprocess_job.id, 20)
            .await
            .expect("reprocess job should succeed");

        assert_eq!(report.queued_jobs.len(), 1);
        assert_eq!(
            report.queued_jobs[0].job_type,
            JobType::AnalyzeReleaseInstance
        );
        assert_eq!(
            report.queued_jobs[0].subject,
            JobSubject::ImportBatch(repository.batch_id())
        );
        assert_eq!(
            repository.release_instance().state,
            ReleaseInstanceState::Staged
        );
        assert_eq!(repository.batch_status(), ImportBatchStatus::Created);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rescan_job_discovers_watch_batches_for_named_watcher() {
        let root = test_root("pipeline-rescan");
        let watch_dir = root.join("incoming/watch");
        let album_dir = watch_dir.join("Kid A");
        fs::create_dir_all(&album_dir).expect("album dir should exist");
        let mp3_path = album_dir.join("01 - Everything in Its Right Place.mp3");
        seed_mp3(
            &mp3_path,
            "Radiohead",
            "Kid A",
            "Everything in Its Right Place",
        );

        let repository = InMemoryPipelineRepository::for_batch_pipeline(&watch_dir, Vec::new());
        let mut config = AppConfig::default();
        config.storage.watch_directories = vec![crate::config::WatchDirectoryConfig {
            name: "watch".to_string(),
            path: watch_dir.clone(),
            scan_mode: crate::config::WatchScanMode::EventDriven,
            import_mode_override: None,
        }];
        let service = JobPipelineService::new(
            repository.clone(),
            FakePipelineProvider::default(),
            ValidatedRuntimeConfig::from_validated_app_config(&config),
            crate::application::observability::ObservabilityContext::default(),
            WorkerPools::from_config(&config.workers),
        );
        let rescan_job = repository.insert_job(Job::queued(
            JobType::RescanWatcher,
            JobSubject::SourceScan("watch".to_string()),
            JobTrigger::Operator,
            10,
        ));

        let report = service
            .run_job(&rescan_job.id, 20)
            .await
            .expect("rescan job should succeed");

        assert_eq!(report.queued_jobs.len(), 1);
        assert_eq!(report.queued_jobs[0].job_type, JobType::DiscoverBatch);

        let _ = fs::remove_dir_all(root);
    }

    #[derive(Clone)]
    struct FakePipelineProvider;

    impl Default for FakePipelineProvider {
        fn default() -> Self {
            Self
        }
    }

    impl MusicBrainzMetadataProvider for FakePipelineProvider {
        fn search_releases(
            &self,
            _query: &str,
            _limit: u8,
        ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseCandidate>, String>> + Send {
            async move {
                Ok(vec![MusicBrainzReleaseCandidate {
                    id: "release-1".to_string(),
                    title: "Kid A".to_string(),
                    score: 100,
                    artist_names: vec!["Radiohead".to_string()],
                    release_group_id: Some("group-1".to_string()),
                    release_group_title: Some("Kid A".to_string()),
                    country: Some("GB".to_string()),
                    date: Some("2000-10-02".to_string()),
                    track_count: Some(1),
                }])
            }
        }

        fn search_release_groups(
            &self,
            _query: &str,
            _limit: u8,
        ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseGroupCandidate>, String>> + Send
        {
            async move {
                Ok(vec![MusicBrainzReleaseGroupCandidate {
                    id: "group-1".to_string(),
                    title: "Kid A".to_string(),
                    score: 97,
                    artist_names: vec!["Radiohead".to_string()],
                    primary_type: Some("Album".to_string()),
                    first_release_date: Some("2000".to_string()),
                }])
            }
        }

        fn lookup_release(
            &self,
            release_id: &str,
        ) -> impl Future<Output = Result<MusicBrainzReleaseDetail, String>> + Send {
            let release_id = release_id.to_string();
            async move {
                Ok(MusicBrainzReleaseDetail {
                    id: release_id,
                    title: "Kid A".to_string(),
                    country: Some("GB".to_string()),
                    date: Some("2000-10-02".to_string()),
                    artist_credit: vec![MusicBrainzArtistCredit {
                        artist_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_string(),
                        artist_name: "Radiohead".to_string(),
                        artist_sort_name: "Radiohead".to_string(),
                    }],
                    release_group: Some(MusicBrainzReleaseGroupRef {
                        id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".to_string(),
                        title: "Kid A".to_string(),
                        primary_type: Some("Album".to_string()),
                    }),
                    label_info: vec![MusicBrainzLabelInfo {
                        catalog_number: Some("XLLP782".to_string()),
                        label_name: Some("XL Recordings".to_string()),
                    }],
                })
            }
        }
    }

    impl DiscogsMetadataProvider for FakePipelineProvider {
        fn search_releases(
            &self,
            _query: &DiscogsReleaseQuery,
            _limit: u8,
        ) -> impl Future<Output = Result<Vec<DiscogsReleaseCandidate>, String>> + Send {
            async move {
                Ok(vec![DiscogsReleaseCandidate {
                    id: "discogs-1".to_string(),
                    title: "Kid A".to_string(),
                    artist: Some("Radiohead".to_string()),
                    year: Some("2000".to_string()),
                    country: Some("UK".to_string()),
                    label: Some("XL Recordings".to_string()),
                    catalog_number: Some("XLLP782".to_string()),
                    format_descriptors: vec!["CD".to_string(), "Album".to_string()],
                    raw_payload: "{\"id\":1}".to_string(),
                }])
            }
        }
    }

    #[derive(Clone)]
    struct InMemoryPipelineRepository {
        sources: Arc<Mutex<HashMap<String, Source>>>,
        batches: Arc<Mutex<HashMap<String, ImportBatch>>>,
        manifests: Arc<Mutex<Vec<StagingManifest>>>,
        evidence: Arc<Mutex<Vec<IngestEvidenceRecord>>>,
        metadata_snapshots: Arc<Mutex<Vec<MetadataSnapshot>>>,
        artists: Arc<Mutex<Vec<Artist>>>,
        release_groups: Arc<Mutex<Vec<ReleaseGroup>>>,
        releases: Arc<Mutex<Vec<Release>>>,
        tracks: Arc<Mutex<HashMap<ReleaseId, Vec<Track>>>>,
        release_instances: Arc<Mutex<Vec<ReleaseInstance>>>,
        candidate_matches: Arc<Mutex<HashMap<ReleaseInstanceId, Vec<CandidateMatch>>>>,
        manual_overrides: Arc<Mutex<Vec<ManualOverride>>>,
        exports: Arc<Mutex<Vec<ExportedMetadataSnapshot>>>,
        artworks: Arc<Mutex<Vec<ReleaseArtwork>>>,
        track_instances: Arc<Mutex<Vec<TrackInstance>>>,
        files: Arc<Mutex<Vec<FileRecord>>>,
        issues: Arc<Mutex<Vec<Issue>>>,
        jobs: Arc<Mutex<HashMap<String, Job>>>,
        batch_id: ImportBatchId,
        release_instance_id: ReleaseInstanceId,
        source_id: SourceId,
        release_id: ReleaseId,
        release_group_id: ReleaseGroupId,
    }

    impl InMemoryPipelineRepository {
        fn for_batch_pipeline(source_dir: &Path, received_paths: Vec<PathBuf>) -> Self {
            let source_id = SourceId::new();
            let batch_id = ImportBatchId::new();
            let source = Source {
                id: source_id.clone(),
                kind: SourceKind::ManualAdd,
                display_name: "manual".to_string(),
                locator: SourceLocator::ManualEntry {
                    submitted_path: source_dir.to_path_buf(),
                },
                external_reference: None,
            };
            let batch = ImportBatch {
                id: batch_id.clone(),
                source_id: source_id.clone(),
                mode: ImportMode::Copy,
                status: ImportBatchStatus::Created,
                requested_by: BatchRequester::Operator {
                    name: "operator".to_string(),
                },
                created_at_unix_seconds: 1,
                received_paths,
            };
            Self::base(source, batch)
        }

        fn for_release_instance_pipeline(source_dir: &Path, source_path: PathBuf) -> Self {
            let source_id = SourceId::new();
            let batch_id = ImportBatchId::new();
            let source = Source {
                id: source_id.clone(),
                kind: SourceKind::ManualAdd,
                display_name: "Incoming".to_string(),
                locator: SourceLocator::ManualEntry {
                    submitted_path: source_dir.to_path_buf(),
                },
                external_reference: None,
            };
            let batch = ImportBatch {
                id: batch_id.clone(),
                source_id: source_id.clone(),
                mode: ImportMode::Copy,
                status: ImportBatchStatus::Grouped,
                requested_by: BatchRequester::Operator {
                    name: "operator".to_string(),
                },
                created_at_unix_seconds: 1,
                received_paths: vec![source_dir.to_path_buf()],
            };
            let repository = Self::base(source, batch);
            repository.seed_release_graph(source_path);
            repository
        }

        fn base(source: Source, batch: ImportBatch) -> Self {
            let release_group_id = ReleaseGroupId::new();
            let release_id = ReleaseId::new();
            let release_instance_id = ReleaseInstanceId::new();
            let source_id = source.id.clone();
            let mut sources = HashMap::new();
            sources.insert(source.id.as_uuid().to_string(), source);
            let mut batches = HashMap::new();
            batches.insert(batch.id.as_uuid().to_string(), batch.clone());
            Self {
                sources: Arc::new(Mutex::new(sources)),
                batches: Arc::new(Mutex::new(batches)),
                manifests: Arc::new(Mutex::new(Vec::new())),
                evidence: Arc::new(Mutex::new(Vec::new())),
                metadata_snapshots: Arc::new(Mutex::new(Vec::new())),
                artists: Arc::new(Mutex::new(Vec::new())),
                release_groups: Arc::new(Mutex::new(Vec::new())),
                releases: Arc::new(Mutex::new(Vec::new())),
                tracks: Arc::new(Mutex::new(HashMap::new())),
                release_instances: Arc::new(Mutex::new(Vec::new())),
                candidate_matches: Arc::new(Mutex::new(HashMap::new())),
                manual_overrides: Arc::new(Mutex::new(Vec::new())),
                exports: Arc::new(Mutex::new(Vec::new())),
                artworks: Arc::new(Mutex::new(Vec::new())),
                track_instances: Arc::new(Mutex::new(Vec::new())),
                files: Arc::new(Mutex::new(Vec::new())),
                issues: Arc::new(Mutex::new(Vec::new())),
                jobs: Arc::new(Mutex::new(HashMap::new())),
                batch_id: batch.id,
                release_instance_id,
                source_id,
                release_id,
                release_group_id,
            }
        }

        fn seed_release_graph(&self, source_path: PathBuf) {
            let artist_id = ArtistId::new();
            self.release_groups
                .lock()
                .expect("release groups should lock")
                .push(ReleaseGroup {
                    id: self.release_group_id.clone(),
                    primary_artist_id: artist_id.clone(),
                    title: "Kid A".to_string(),
                    kind: ReleaseGroupKind::Album,
                    musicbrainz_release_group_id: MusicBrainzReleaseGroupId::parse_str(
                        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                    )
                    .ok(),
                });
            self.releases
                .lock()
                .expect("releases should lock")
                .push(Release {
                    id: self.release_id.clone(),
                    release_group_id: self.release_group_id.clone(),
                    primary_artist_id: artist_id,
                    title: "Kid A".to_string(),
                    musicbrainz_release_id: MusicBrainzReleaseId::parse_str(
                        "55555555-5555-4555-8555-555555555555",
                    )
                    .ok(),
                    discogs_release_id: None,
                    edition: ReleaseEdition {
                        release_date: Some(PartialDate {
                            year: 2000,
                            month: Some(10),
                            day: Some(2),
                        }),
                        ..ReleaseEdition::default()
                    },
                });
            self.tracks.lock().expect("tracks should lock").insert(
                self.release_id.clone(),
                vec![Track {
                    id: TrackId::new(),
                    release_id: self.release_id.clone(),
                    position: TrackPosition {
                        disc_number: 1,
                        track_number: 1,
                    },
                    title: "Everything in Its Right Place".to_string(),
                    musicbrainz_track_id: MusicBrainzTrackId::parse_str(
                        "66666666-6666-4666-8666-666666666666",
                    )
                    .ok(),
                    duration_ms: Some(250_000),
                }],
            );
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .push(ReleaseInstance {
                    id: self.release_instance_id.clone(),
                    import_batch_id: self.batch_id.clone(),
                    source_id: self.source_id.clone(),
                    release_id: Some(self.release_id.clone()),
                    state: ReleaseInstanceState::Matched,
                    technical_variant: TechnicalVariant {
                        format_family: FormatFamily::Mp3,
                        bitrate_mode: BitrateMode::Variable,
                        bitrate_kbps: Some(320),
                        sample_rate_hz: Some(44_100),
                        bit_depth: None,
                        track_count: 1,
                        total_duration_seconds: 250,
                    },
                    provenance: ProvenanceSnapshot {
                        ingest_origin: IngestOrigin::ManualAdd,
                        original_source_path: source_path
                            .parent()
                            .expect("parent")
                            .display()
                            .to_string(),
                        imported_at_unix_seconds: 1,
                        gazelle_reference: None,
                    },
                });
            let artwork_path = source_path.parent().expect("parent").join("folder.jpg");
            fs::write(&artwork_path, b"jpeg-data").expect("artwork should write");
            self.manifests
                .lock()
                .expect("manifests should lock")
                .push(StagingManifest {
                    id: StagingManifestId::new(),
                    batch_id: self.batch_id.clone(),
                    source: StagingManifestSource {
                        kind: SourceKind::ManualAdd,
                        source_path: source_path.parent().expect("parent").to_path_buf(),
                    },
                    discovered_files: Vec::new(),
                    auxiliary_files: vec![AuxiliaryFile {
                        path: artwork_path.clone(),
                        role: AuxiliaryFileRole::Artwork,
                    }],
                    grouping: GroupingDecision {
                        strategy: GroupingStrategy::CommonParentDirectory,
                        groups: vec![StagedReleaseGroup {
                            key: "kid-a".to_string(),
                            file_paths: vec![source_path],
                            auxiliary_paths: vec![artwork_path],
                        }],
                        notes: Vec::new(),
                    },
                    captured_at_unix_seconds: 1,
                });
        }

        fn seed_export_snapshot(&self) {
            self.exports
                .lock()
                .expect("exports should lock")
                .push(ExportedMetadataSnapshot {
                    id: ExportedMetadataSnapshotId::new(),
                    release_instance_id: self.release_instance_id.clone(),
                    export_profile: "generic_player".to_string(),
                    album_title: "Kid A [2000]".to_string(),
                    album_artist: "Radiohead".to_string(),
                    artist_credits: vec!["Radiohead".to_string()],
                    edition_visibility: QualifierVisibility::TagsAndPath,
                    technical_visibility: QualifierVisibility::PathOnly,
                    path_components: vec![
                        "Radiohead".to_string(),
                        "Kid A [2000] [MP3 320kbps]".to_string(),
                    ],
                    primary_artwork_filename: Some("cover.jpg".to_string()),
                    compatibility: CompatibilityReport {
                        verified: true,
                        warnings: Vec::new(),
                    },
                    rendered_at_unix_seconds: 1,
                });
        }

        fn seed_duplicate_release_instance(&self) {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .push(ReleaseInstance {
                    id: ReleaseInstanceId::new(),
                    import_batch_id: ImportBatchId::new(),
                    source_id: SourceId::new(),
                    release_id: Some(self.release_id.clone()),
                    state: ReleaseInstanceState::Verified,
                    technical_variant: TechnicalVariant {
                        format_family: FormatFamily::Mp3,
                        bitrate_mode: BitrateMode::Variable,
                        bitrate_kbps: Some(320),
                        sample_rate_hz: Some(44_100),
                        bit_depth: None,
                        track_count: 1,
                        total_duration_seconds: 254,
                    },
                    provenance: ProvenanceSnapshot {
                        ingest_origin: IngestOrigin::ManualAdd,
                        original_source_path: "/tmp/duplicate".to_string(),
                        imported_at_unix_seconds: 1,
                        gazelle_reference: None,
                    },
                });
        }

        fn insert_job(&self, job: Job) -> Job {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            job
        }

        fn batch_id(&self) -> ImportBatchId {
            self.batch_id.clone()
        }
        fn release_instance_id(&self) -> ReleaseInstanceId {
            self.release_instance_id.clone()
        }
        fn release_instance(&self) -> ReleaseInstance {
            self.release_instances
                .lock()
                .expect("release instances should lock")[0]
                .clone()
        }
        fn release_instances(&self) -> Vec<ReleaseInstance> {
            self.release_instances
                .lock()
                .expect("release instances should lock")
                .clone()
        }
        fn batch_status(&self) -> ImportBatchStatus {
            self.batches
                .lock()
                .expect("batches should lock")
                .get(&self.batch_id.as_uuid().to_string())
                .unwrap()
                .status
                .clone()
        }
        fn open_issues(&self) -> Vec<Issue> {
            self.issues
                .lock()
                .expect("issues should lock")
                .iter()
                .filter(|issue| issue.state == IssueState::Open)
                .cloned()
                .collect()
        }
        fn job(&self, job_id: &JobId) -> Job {
            self.jobs
                .lock()
                .expect("jobs should lock")
                .get(&job_id.as_uuid().to_string())
                .unwrap()
                .clone()
        }
        fn managed_files(&self, release_instance_id: ReleaseInstanceId) -> Vec<FileRecord> {
            self.files
                .lock()
                .expect("files should lock")
                .iter()
                .filter(|file| {
                    file.role == FileRole::Managed
                        && self
                            .track_instances
                            .lock()
                            .expect("track instances")
                            .iter()
                            .any(|track| {
                                track.id == file.track_instance_id
                                    && track.release_instance_id == release_instance_id
                            })
                })
                .cloned()
                .collect()
        }
    }

    impl SourceRepository for InMemoryPipelineRepository {
        fn get_source(&self, id: &SourceId) -> Result<Option<Source>, RepositoryError> {
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
    impl SourceCommandRepository for InMemoryPipelineRepository {
        fn create_source(&self, source: &Source) -> Result<(), RepositoryError> {
            self.sources
                .lock()
                .expect("sources should lock")
                .insert(source.id.as_uuid().to_string(), source.clone());
            Ok(())
        }
    }
    impl ImportBatchRepository for InMemoryPipelineRepository {
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
            query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            let items = self
                .batches
                .lock()
                .expect("batches should lock")
                .values()
                .cloned()
                .collect::<Vec<_>>();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }
    impl ImportBatchCommandRepository for InMemoryPipelineRepository {
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
            source_id: &SourceId,
        ) -> Result<Vec<ImportBatch>, RepositoryError> {
            Ok(self
                .batches
                .lock()
                .expect("batches should lock")
                .values()
                .filter(|batch| batch.source_id == *source_id)
                .cloned()
                .collect())
        }
    }
    impl JobRepository for InMemoryPipelineRepository {
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
    impl JobCommandRepository for InMemoryPipelineRepository {
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
    impl StagingManifestRepository for InMemoryPipelineRepository {
        fn list_staging_manifests_for_batch(
            &self,
            batch_id: &ImportBatchId,
        ) -> Result<Vec<StagingManifest>, RepositoryError> {
            Ok(self
                .manifests
                .lock()
                .expect("manifests should lock")
                .iter()
                .filter(|manifest| manifest.batch_id == *batch_id)
                .cloned()
                .collect())
        }
    }
    impl StagingManifestCommandRepository for InMemoryPipelineRepository {
        fn create_staging_manifest(
            &self,
            manifest: &StagingManifest,
        ) -> Result<(), RepositoryError> {
            self.manifests
                .lock()
                .expect("manifests should lock")
                .push(manifest.clone());
            Ok(())
        }
    }
    impl IngestEvidenceRepository for InMemoryPipelineRepository {
        fn list_ingest_evidence_for_batch(
            &self,
            batch_id: &ImportBatchId,
        ) -> Result<Vec<IngestEvidenceRecord>, RepositoryError> {
            Ok(self
                .evidence
                .lock()
                .expect("evidence should lock")
                .iter()
                .filter(|record| record.batch_id == *batch_id)
                .cloned()
                .collect())
        }
    }
    impl IngestEvidenceCommandRepository for InMemoryPipelineRepository {
        fn create_ingest_evidence_records(
            &self,
            records: &[IngestEvidenceRecord],
        ) -> Result<(), RepositoryError> {
            self.evidence
                .lock()
                .expect("evidence should lock")
                .extend(records.iter().cloned());
            Ok(())
        }
    }
    impl MetadataSnapshotRepository for InMemoryPipelineRepository {
        fn list_metadata_snapshots_for_batch(
            &self,
            batch_id: &ImportBatchId,
        ) -> Result<Vec<MetadataSnapshot>, RepositoryError> {
            Ok(self.metadata_snapshots.lock().expect("metadata snapshots should lock").iter().filter(|snapshot| matches!(snapshot.subject, MetadataSubject::ImportBatch(ref id) if id == batch_id)).cloned().collect())
        }
        fn list_metadata_snapshots_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<MetadataSnapshot>, RepositoryError> {
            Ok(self.metadata_snapshots.lock().expect("metadata snapshots should lock").iter().filter(|snapshot| matches!(snapshot.subject, MetadataSubject::ReleaseInstance(ref id) if id == release_instance_id)).cloned().collect())
        }
    }
    impl MetadataSnapshotCommandRepository for InMemoryPipelineRepository {
        fn create_metadata_snapshots(
            &self,
            snapshots: &[MetadataSnapshot],
        ) -> Result<(), RepositoryError> {
            self.metadata_snapshots
                .lock()
                .expect("metadata snapshots should lock")
                .extend(snapshots.iter().cloned());
            Ok(())
        }
    }
    impl ReleaseCommandRepository for InMemoryPipelineRepository {
        fn create_artist(&self, artist: &Artist) -> Result<(), RepositoryError> {
            self.artists
                .lock()
                .expect("artists should lock")
                .push(artist.clone());
            Ok(())
        }
        fn create_release_group(
            &self,
            release_group: &ReleaseGroup,
        ) -> Result<(), RepositoryError> {
            self.release_groups
                .lock()
                .expect("release groups should lock")
                .push(release_group.clone());
            Ok(())
        }
        fn create_release(&self, release: &Release) -> Result<(), RepositoryError> {
            self.releases
                .lock()
                .expect("releases should lock")
                .push(release.clone());
            Ok(())
        }
    }
    impl ReleaseRepository for InMemoryPipelineRepository {
        fn find_artist_by_musicbrainz_id(
            &self,
            musicbrainz_artist_id: &str,
        ) -> Result<Option<Artist>, RepositoryError> {
            Ok(self
                .artists
                .lock()
                .expect("artists should lock")
                .iter()
                .find(|artist| {
                    artist
                        .musicbrainz_artist_id
                        .as_ref()
                        .map(|id| id.as_uuid().to_string())
                        == Some(musicbrainz_artist_id.to_string())
                })
                .cloned())
        }
        fn get_release_group(
            &self,
            id: &ReleaseGroupId,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .iter()
                .find(|group| group.id == *id)
                .cloned())
        }
        fn find_release_group_by_musicbrainz_id(
            &self,
            musicbrainz_release_group_id: &str,
        ) -> Result<Option<ReleaseGroup>, RepositoryError> {
            Ok(self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .iter()
                .find(|group| {
                    group
                        .musicbrainz_release_group_id
                        .as_ref()
                        .map(|id| id.as_uuid().to_string())
                        == Some(musicbrainz_release_group_id.to_string())
                })
                .cloned())
        }
        fn get_release(&self, id: &ReleaseId) -> Result<Option<Release>, RepositoryError> {
            Ok(self
                .releases
                .lock()
                .expect("releases should lock")
                .iter()
                .find(|release| release.id == *id)
                .cloned())
        }
        fn find_release_by_musicbrainz_id(
            &self,
            musicbrainz_release_id: &str,
        ) -> Result<Option<Release>, RepositoryError> {
            Ok(self
                .releases
                .lock()
                .expect("releases should lock")
                .iter()
                .find(|release| {
                    release
                        .musicbrainz_release_id
                        .as_ref()
                        .map(|id| id.as_uuid().to_string())
                        == Some(musicbrainz_release_id.to_string())
                })
                .cloned())
        }
        fn search_release_groups(
            &self,
            query: &ReleaseGroupSearchQuery,
        ) -> Result<Page<ReleaseGroup>, RepositoryError> {
            let items = self
                .release_groups
                .lock()
                .expect("release groups should lock")
                .clone();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
        fn list_releases(
            &self,
            query: &ReleaseListQuery,
        ) -> Result<Page<Release>, RepositoryError> {
            let mut items = self.releases.lock().expect("releases should lock").clone();
            if let Some(id) = &query.release_group_id {
                items.retain(|release| &release.release_group_id == id);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
        fn list_tracks_for_release(
            &self,
            release_id: &ReleaseId,
        ) -> Result<Vec<Track>, RepositoryError> {
            Ok(self
                .tracks
                .lock()
                .expect("tracks should lock")
                .get(release_id)
                .cloned()
                .unwrap_or_default())
        }
    }
    impl ReleaseInstanceRepository for InMemoryPipelineRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .iter()
                .find(|release_instance| release_instance.id == *id)
                .cloned())
        }
        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            let mut items = self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .clone();
            if let Some(release_id) = &query.release_id {
                items.retain(|item| item.release_id.as_ref() == Some(release_id));
            }
            if let Some(state) = &query.state {
                items.retain(|item| &item.state == state);
            }
            if let Some(format_family) = &query.format_family {
                items.retain(|item| &item.technical_variant.format_family == format_family);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
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
                .iter()
                .filter(|item| item.import_batch_id == *import_batch_id)
                .cloned()
                .collect())
        }
        fn list_candidate_matches(
            &self,
            release_instance_id: &ReleaseInstanceId,
            page: &PageRequest,
        ) -> Result<Page<CandidateMatch>, RepositoryError> {
            let items = self
                .candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .get(release_instance_id)
                .cloned()
                .unwrap_or_default();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: *page,
            })
        }
        fn get_candidate_match(
            &self,
            id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<CandidateMatch>, RepositoryError> {
            Ok(self
                .candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .values()
                .flatten()
                .find(|candidate| candidate.id == *id)
                .cloned())
        }
        fn list_track_instances_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<TrackInstance>, RepositoryError> {
            Ok(self
                .track_instances
                .lock()
                .expect("track instances should lock")
                .iter()
                .filter(|item| item.release_instance_id == *release_instance_id)
                .cloned()
                .collect())
        }
        fn list_files_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
            role: Option<FileRole>,
        ) -> Result<Vec<FileRecord>, RepositoryError> {
            let track_ids = self
                .track_instances
                .lock()
                .expect("track instances should lock")
                .iter()
                .filter(|item| item.release_instance_id == *release_instance_id)
                .map(|item| item.id.clone())
                .collect::<Vec<_>>();
            Ok(self
                .files
                .lock()
                .expect("files should lock")
                .iter()
                .filter(|file| {
                    track_ids
                        .iter()
                        .any(|track_id| *track_id == file.track_instance_id)
                })
                .filter(|file| role.as_ref().is_none_or(|expected| &file.role == expected))
                .cloned()
                .collect())
        }
    }
    impl ReleaseInstanceCommandRepository for InMemoryPipelineRepository {
        fn create_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            let mut items = self
                .release_instances
                .lock()
                .expect("release instances should lock");
            if let Some(existing) = items.iter_mut().find(|item| item.id == release_instance.id) {
                *existing = release_instance.clone();
            } else {
                items.push(release_instance.clone());
            }
            Ok(())
        }
        fn update_release_instance(
            &self,
            release_instance: &ReleaseInstance,
        ) -> Result<(), RepositoryError> {
            self.create_release_instance(release_instance)
        }
        fn replace_candidate_matches(
            &self,
            release_instance_id: &ReleaseInstanceId,
            matches: &[CandidateMatch],
        ) -> Result<(), RepositoryError> {
            self.candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .insert(release_instance_id.clone(), matches.to_vec());
            Ok(())
        }
        fn replace_candidate_matches_for_provider(
            &self,
            release_instance_id: &ReleaseInstanceId,
            provider: &CandidateProvider,
            matches: &[CandidateMatch],
        ) -> Result<(), RepositoryError> {
            let mut candidates = self
                .candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .get(release_instance_id)
                .cloned()
                .unwrap_or_default();
            candidates.retain(|candidate| &candidate.provider != provider);
            candidates.extend_from_slice(matches);
            self.candidate_matches
                .lock()
                .expect("candidate matches should lock")
                .insert(release_instance_id.clone(), candidates);
            Ok(())
        }
        fn replace_track_instances_and_files(
            &self,
            release_instance_id: &ReleaseInstanceId,
            track_instances: &[TrackInstance],
            files: &[FileRecord],
        ) -> Result<(), RepositoryError> {
            self.track_instances
                .lock()
                .expect("track instances should lock")
                .retain(|item| item.release_instance_id != *release_instance_id);
            self.track_instances
                .lock()
                .expect("track instances should lock")
                .extend_from_slice(track_instances);
            let valid_track_ids = track_instances
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>();
            self.files
                .lock()
                .expect("files should lock")
                .retain(|file| {
                    !valid_track_ids
                        .iter()
                        .any(|id| *id == file.track_instance_id)
                });
            self.files
                .lock()
                .expect("files should lock")
                .extend_from_slice(files);
            Ok(())
        }
    }
    impl ManualOverrideRepository for InMemoryPipelineRepository {
        fn get_manual_override(
            &self,
            id: &ManualOverrideId,
        ) -> Result<Option<ManualOverride>, RepositoryError> {
            Ok(self
                .manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .iter()
                .find(|item| item.id == *id)
                .cloned())
        }
        fn list_manual_overrides(
            &self,
            query: &ManualOverrideListQuery,
        ) -> Result<Page<ManualOverride>, RepositoryError> {
            let items = self
                .manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .iter()
                .filter(|item| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &item.subject == subject)
                })
                .filter(|item| {
                    query
                        .field
                        .as_ref()
                        .is_none_or(|field| &item.field == field)
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
    impl ManualOverrideCommandRepository for InMemoryPipelineRepository {
        fn create_manual_override(
            &self,
            override_record: &ManualOverride,
        ) -> Result<(), RepositoryError> {
            self.manual_overrides
                .lock()
                .expect("manual overrides should lock")
                .push(override_record.clone());
            Ok(())
        }
    }
    impl ExportRepository for InMemoryPipelineRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .lock()
                .expect("exports should lock")
                .iter()
                .filter(|item| item.release_instance_id == *release_instance_id)
                .max_by_key(|item| item.rendered_at_unix_seconds)
                .cloned())
        }
        fn list_exported_metadata(
            &self,
            query: &ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            let mut items = self.exports.lock().expect("exports should lock").clone();
            if let Some(release_instance_id) = &query.release_instance_id {
                items.retain(|item| &item.release_instance_id == release_instance_id);
            }
            if let Some(album_title) = &query.album_title {
                items.retain(|item| &item.album_title == album_title);
            }
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
        fn get_exported_metadata(
            &self,
            id: &ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(self
                .exports
                .lock()
                .expect("exports should lock")
                .iter()
                .find(|item| item.id == *id)
                .cloned())
        }
    }
    impl ExportCommandRepository for InMemoryPipelineRepository {
        fn create_exported_metadata_snapshot(
            &self,
            snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            self.exports
                .lock()
                .expect("exports should lock")
                .push(snapshot.clone());
            Ok(())
        }
        fn update_exported_metadata_snapshot(
            &self,
            snapshot: &ExportedMetadataSnapshot,
        ) -> Result<(), RepositoryError> {
            let mut items = self.exports.lock().expect("exports should lock");
            let stored = items
                .iter_mut()
                .find(|item| item.id == snapshot.id)
                .ok_or_else(|| RepositoryError {
                    kind: RepositoryErrorKind::NotFound,
                    message: "snapshot not found".to_string(),
                })?;
            *stored = snapshot.clone();
            Ok(())
        }
    }
    impl crate::application::repository::ReleaseArtworkRepository for InMemoryPipelineRepository {
        fn get_release_artwork(
            &self,
            id: &crate::support::ids::ReleaseArtworkId,
        ) -> Result<Option<ReleaseArtwork>, RepositoryError> {
            Ok(self
                .artworks
                .lock()
                .expect("artworks should lock")
                .iter()
                .find(|item| item.id == *id)
                .cloned())
        }

        fn list_release_artwork_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<ReleaseArtwork>, RepositoryError> {
            Ok(self
                .artworks
                .lock()
                .expect("artworks should lock")
                .iter()
                .filter(|item| item.release_instance_id.as_ref() == Some(release_instance_id))
                .cloned()
                .collect())
        }
    }
    impl crate::application::repository::ReleaseArtworkCommandRepository
        for InMemoryPipelineRepository
    {
        fn replace_release_artwork_for_release_instance(
            &self,
            release_instance_id: &ReleaseInstanceId,
            artwork: &[ReleaseArtwork],
        ) -> Result<(), RepositoryError> {
            let mut items = self.artworks.lock().expect("artworks should lock");
            items.retain(|item| item.release_instance_id.as_ref() != Some(release_instance_id));
            items.extend_from_slice(artwork);
            Ok(())
        }
    }
    impl IssueRepository for InMemoryPipelineRepository {
        fn get_issue(
            &self,
            id: &crate::support::ids::IssueId,
        ) -> Result<Option<Issue>, RepositoryError> {
            Ok(self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .find(|item| item.id == *id)
                .cloned())
        }
        fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            let items = self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .filter(|issue| {
                    query
                        .state
                        .as_ref()
                        .is_none_or(|state| &issue.state == state)
                })
                .filter(|issue| {
                    query
                        .issue_type
                        .as_ref()
                        .is_none_or(|issue_type| &issue.issue_type == issue_type)
                })
                .filter(|issue| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &issue.subject == subject)
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
    impl IssueCommandRepository for InMemoryPipelineRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .push(issue.clone());
            Ok(())
        }
        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            let mut issues = self.issues.lock().expect("issues should lock");
            if let Some(stored) = issues.iter_mut().find(|stored| stored.id == issue.id) {
                *stored = issue.clone();
            } else {
                issues.push(issue.clone());
            }
            Ok(())
        }
    }

    fn seed_mp3(path: &Path, artist: &str, album: &str, title: &str) {
        fs::write(path, b"mp3 audio").expect("mp3 should exist");
        let mut tag = id3::Tag::new();
        tag.set_artist(artist);
        tag.set_album(album);
        tag.set_title(title);
        tag.set_track(1);
        tag.set_disc(1);
        tag.set_year(2000);
        tag.write_to_path(path, id3::Version::Id3v24)
            .expect("id3 tag should write");
    }

    fn test_root(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("discern-{label}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("temp root should exist");
        root
    }
}
