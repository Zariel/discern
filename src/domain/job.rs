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
