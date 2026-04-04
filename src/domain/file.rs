use std::path::PathBuf;

use crate::domain::release_instance::FormatFamily;
use crate::support::ids::{FileId, TrackInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecord {
    pub id: FileId,
    pub track_instance_id: TrackInstanceId,
    pub role: FileRole,
    pub format_family: FormatFamily,
    pub path: PathBuf,
    pub checksum: Option<String>,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileRole {
    Source,
    Managed,
}
