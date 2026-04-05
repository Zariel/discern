use crate::application::repository::{
    JobCommandRepository, JobRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::job::{Job, JobLifecycleError, JobSubject, JobTrigger, JobType, RetryScope};
use crate::support::ids::JobId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobServiceError {
    pub kind: JobServiceErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobServiceErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct JobService<R> {
    repository: R,
}

impl<R> JobService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> JobService<R>
where
    R: JobRepository + JobCommandRepository,
{
    pub fn enqueue_job(
        &self,
        job_type: JobType,
        subject: JobSubject,
        triggered_by: JobTrigger,
        created_at_unix_seconds: i64,
    ) -> Result<Job, JobServiceError> {
        let job = Job::queued(job_type, subject, triggered_by, created_at_unix_seconds);
        self.repository
            .create_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }

    pub fn start_job(
        &self,
        job_id: &JobId,
        phase: impl Into<String>,
        started_at_unix_seconds: i64,
    ) -> Result<Job, JobServiceError> {
        let mut job = load_job(&self.repository, job_id)?;
        job.start(phase, started_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }

    pub fn complete_job(
        &self,
        job_id: &JobId,
        phase: impl Into<String>,
        finished_at_unix_seconds: i64,
    ) -> Result<Job, JobServiceError> {
        let mut job = load_job(&self.repository, job_id)?;
        job.succeed(phase, finished_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }

    pub fn fail_job(
        &self,
        job_id: &JobId,
        phase: impl Into<String>,
        error_payload: impl Into<String>,
        finished_at_unix_seconds: i64,
    ) -> Result<Job, JobServiceError> {
        let mut job = load_job(&self.repository, job_id)?;
        job.fail(phase, error_payload, finished_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }

    pub fn mark_job_resumable(
        &self,
        job_id: &JobId,
        phase: impl Into<String>,
        reason: impl Into<String>,
        finished_at_unix_seconds: i64,
    ) -> Result<Job, JobServiceError> {
        let mut job = load_job(&self.repository, job_id)?;
        job.mark_resumable(phase, reason, finished_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }

    pub fn retry_job(
        &self,
        job_id: &JobId,
        scope: RetryScope,
        queued_at_unix_seconds: i64,
    ) -> Result<Job, JobServiceError> {
        let mut job = load_job(&self.repository, job_id)?;
        job.retry(scope, queued_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_job(&job)
            .map_err(map_repository_error)?;
        Ok(job)
    }

    pub fn recover_unfinished_jobs(
        &self,
        recovered_at_unix_seconds: i64,
    ) -> Result<Vec<Job>, JobServiceError> {
        let jobs = self
            .repository
            .list_recoverable_jobs()
            .map_err(map_repository_error)?;
        let mut recovered = Vec::with_capacity(jobs.len());

        for mut job in jobs {
            job.mark_resumable(
                "recovery",
                "recovered during startup",
                recovered_at_unix_seconds,
            )
            .map_err(map_lifecycle_error)?;
            self.repository
                .update_job(&job)
                .map_err(map_repository_error)?;
            recovered.push(job);
        }

        Ok(recovered)
    }
}

fn load_job<R>(repository: &R, job_id: &JobId) -> Result<Job, JobServiceError>
where
    R: JobRepository,
{
    repository
        .get_job(job_id)
        .map_err(map_repository_error)?
        .ok_or_else(|| JobServiceError {
            kind: JobServiceErrorKind::NotFound,
            message: format!("job {} was not found", job_id.as_uuid()),
        })
}

fn map_repository_error(error: RepositoryError) -> JobServiceError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => JobServiceErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            JobServiceErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => JobServiceErrorKind::Storage,
    };
    JobServiceError {
        kind,
        message: error.message,
    }
}

fn map_lifecycle_error(error: JobLifecycleError) -> JobServiceError {
    JobServiceError {
        kind: JobServiceErrorKind::Conflict,
        message: match error {
            JobLifecycleError::AlreadyQueued => "job is already queued".to_string(),
            JobLifecycleError::AlreadyRunning => "job is already running".to_string(),
            JobLifecycleError::AlreadyResumable => "job is already resumable".to_string(),
            JobLifecycleError::CompletedJobCannotRestart => {
                "completed jobs cannot restart".to_string()
            }
            JobLifecycleError::FailedJobMustRetryFirst => {
                "failed jobs must retry before restarting".to_string()
            }
            JobLifecycleError::OnlyRunningJobsCanComplete => {
                "only running jobs can complete".to_string()
            }
            JobLifecycleError::OnlyRunningJobsCanFail => "only running jobs can fail".to_string(),
            JobLifecycleError::RunningJobCannotRetry => "running jobs cannot retry".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::application::repository::JobListQuery;
    use crate::domain::job::JobStatus;
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn service_enqueues_retries_and_recovers_jobs() {
        let repository = InMemoryJobRepository::default();
        let service = JobService::new(repository.clone());

        let queued = service
            .enqueue_job(
                JobType::MatchReleaseInstance,
                JobSubject::SourceScan("watch".to_string()),
                JobTrigger::System,
                100,
            )
            .expect("enqueue should succeed");
        assert_eq!(queued.status, JobStatus::Queued);

        service
            .start_job(&queued.id, "matching", 101)
            .expect("start should succeed");
        service
            .fail_job(&queued.id, "matching", "timeout", 102)
            .expect("fail should succeed");

        let retried = service
            .retry_job(&queued.id, RetryScope::Rematch, 103)
            .expect("retry should succeed");
        assert_eq!(retried.status, JobStatus::Queued);
        assert_eq!(retried.retry_count, 1);

        let second = service
            .enqueue_job(
                JobType::VerifyImport,
                JobSubject::SourceScan("verify".to_string()),
                JobTrigger::Operator,
                110,
            )
            .expect("enqueue should succeed");
        service
            .start_job(&second.id, "verifying", 111)
            .expect("start should succeed");

        let recovered = service
            .recover_unfinished_jobs(120)
            .expect("recovery should succeed");
        assert_eq!(recovered.len(), 2);
        assert!(
            recovered
                .iter()
                .all(|job| job.status == JobStatus::Resumable)
        );
        assert!(
            recovered
                .iter()
                .all(|job| { job.error_payload == Some("recovered during startup".to_string()) })
        );
    }

    #[derive(Clone, Default)]
    struct InMemoryJobRepository {
        jobs: Arc<Mutex<HashMap<String, Job>>>,
    }

    impl JobRepository for InMemoryJobRepository {
        fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("repository should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_jobs(&self, _query: &JobListQuery) -> Result<Page<Job>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: Default::default(),
                total: 0,
            })
        }
    }

    impl JobCommandRepository for InMemoryJobRepository {
        fn create_job(&self, job: &Job) -> Result<(), RepositoryError> {
            self.jobs
                .lock()
                .expect("repository should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            Ok(())
        }

        fn update_job(&self, job: &Job) -> Result<(), RepositoryError> {
            self.jobs
                .lock()
                .expect("repository should lock")
                .insert(job.id.as_uuid().to_string(), job.clone());
            Ok(())
        }

        fn list_recoverable_jobs(&self) -> Result<Vec<Job>, RepositoryError> {
            Ok(self
                .jobs
                .lock()
                .expect("repository should lock")
                .values()
                .filter(|job| matches!(job.status, JobStatus::Queued | JobStatus::Running))
                .cloned()
                .collect())
        }
    }
}
