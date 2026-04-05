use crate::support::ids::{ImportBatchId, JobId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub job_type: JobType,
    pub subject: JobSubject,
    pub status: JobStatus,
    pub progress_phase: String,
    pub retry_count: u16,
    pub triggered_by: JobTrigger,
    pub created_at_unix_seconds: i64,
    pub started_at_unix_seconds: Option<i64>,
    pub finished_at_unix_seconds: Option<i64>,
    pub error_payload: Option<String>,
}

impl Job {
    pub fn queued(
        job_type: JobType,
        subject: JobSubject,
        triggered_by: JobTrigger,
        created_at_unix_seconds: i64,
    ) -> Self {
        Self {
            id: JobId::new(),
            job_type,
            subject,
            status: JobStatus::Queued,
            progress_phase: "queued".to_string(),
            retry_count: 0,
            triggered_by,
            created_at_unix_seconds,
            started_at_unix_seconds: None,
            finished_at_unix_seconds: None,
            error_payload: None,
        }
    }

    pub fn start(
        &mut self,
        progress_phase: impl Into<String>,
        started_at_unix_seconds: i64,
    ) -> Result<(), JobLifecycleError> {
        match self.status {
            JobStatus::Queued | JobStatus::Resumable => {
                self.status = JobStatus::Running;
                self.progress_phase = progress_phase.into();
                self.started_at_unix_seconds = Some(started_at_unix_seconds);
                self.finished_at_unix_seconds = None;
                self.error_payload = None;
                Ok(())
            }
            JobStatus::Running => Err(JobLifecycleError::AlreadyRunning),
            JobStatus::Succeeded => Err(JobLifecycleError::CompletedJobCannotRestart),
            JobStatus::Failed => Err(JobLifecycleError::FailedJobMustRetryFirst),
        }
    }

    pub fn succeed(
        &mut self,
        progress_phase: impl Into<String>,
        finished_at_unix_seconds: i64,
    ) -> Result<(), JobLifecycleError> {
        match self.status {
            JobStatus::Running => {
                self.status = JobStatus::Succeeded;
                self.progress_phase = progress_phase.into();
                self.finished_at_unix_seconds = Some(finished_at_unix_seconds);
                self.error_payload = None;
                Ok(())
            }
            _ => Err(JobLifecycleError::OnlyRunningJobsCanComplete),
        }
    }

    pub fn fail(
        &mut self,
        progress_phase: impl Into<String>,
        error_payload: impl Into<String>,
        finished_at_unix_seconds: i64,
    ) -> Result<(), JobLifecycleError> {
        match self.status {
            JobStatus::Running => {
                self.status = JobStatus::Failed;
                self.progress_phase = progress_phase.into();
                self.finished_at_unix_seconds = Some(finished_at_unix_seconds);
                self.error_payload = Some(error_payload.into());
                Ok(())
            }
            _ => Err(JobLifecycleError::OnlyRunningJobsCanFail),
        }
    }

    pub fn mark_resumable(
        &mut self,
        progress_phase: impl Into<String>,
        reason: impl Into<String>,
        finished_at_unix_seconds: i64,
    ) -> Result<(), JobLifecycleError> {
        match self.status {
            JobStatus::Queued | JobStatus::Running => {
                self.status = JobStatus::Resumable;
                self.progress_phase = progress_phase.into();
                self.finished_at_unix_seconds = Some(finished_at_unix_seconds);
                self.error_payload = Some(reason.into());
                Ok(())
            }
            JobStatus::Resumable => Err(JobLifecycleError::AlreadyResumable),
            JobStatus::Succeeded => Err(JobLifecycleError::CompletedJobCannotRestart),
            JobStatus::Failed => Err(JobLifecycleError::FailedJobMustRetryFirst),
        }
    }

    pub fn retry(
        &mut self,
        scope: RetryScope,
        queued_at_unix_seconds: i64,
    ) -> Result<(), JobLifecycleError> {
        match self.status {
            JobStatus::Failed | JobStatus::Resumable => {
                self.status = JobStatus::Queued;
                self.progress_phase = scope.default_phase().to_string();
                self.retry_count += 1;
                self.created_at_unix_seconds = queued_at_unix_seconds;
                self.started_at_unix_seconds = None;
                self.finished_at_unix_seconds = None;
                self.error_payload = None;
                Ok(())
            }
            JobStatus::Queued => Err(JobLifecycleError::AlreadyQueued),
            JobStatus::Running => Err(JobLifecycleError::RunningJobCannotRetry),
            JobStatus::Succeeded => Err(JobLifecycleError::CompletedJobCannotRestart),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobType {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Resumable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryScope {
    Reanalyze,
    Rematch,
    RerenderExport,
    Retag,
    Reorganize,
    FullReprocess,
}

impl RetryScope {
    pub fn default_phase(self) -> &'static str {
        match self {
            RetryScope::Reanalyze => "reanalyze",
            RetryScope::Rematch => "rematch",
            RetryScope::RerenderExport => "rerender_export",
            RetryScope::Retag => "retag",
            RetryScope::Reorganize => "reorganize",
            RetryScope::FullReprocess => "reprocess",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobSubject {
    ImportBatch(ImportBatchId),
    ReleaseInstance(ReleaseInstanceId),
    SourceScan(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobTrigger {
    System,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobLifecycleError {
    AlreadyQueued,
    AlreadyRunning,
    AlreadyResumable,
    CompletedJobCannotRestart,
    FailedJobMustRetryFirst,
    OnlyRunningJobsCanComplete,
    OnlyRunningJobsCanFail,
    RunningJobCannotRetry,
}
