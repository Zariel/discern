use std::path::PathBuf;

use crate::support::ids::{ImportBatchId, SourceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportBatch {
    pub id: ImportBatchId,
    pub source_id: SourceId,
    pub mode: ImportMode,
    pub status: ImportBatchStatus,
    pub requested_by: BatchRequester,
    pub created_at_unix_seconds: i64,
    pub received_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportMode {
    Copy,
    Move,
    Hardlink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportBatchStatus {
    Created,
    Discovering,
    Grouped,
    Submitted,
    Quarantined,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchRequester {
    System,
    Operator { name: String },
    ExternalClient { name: String },
}
