use std::path::PathBuf;

use crate::support::ids::SourceId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub id: SourceId,
    pub kind: SourceKind,
    pub display_name: String,
    pub locator: SourceLocator,
    pub external_reference: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceKind {
    WatchDirectory,
    ApiClient,
    ManualAdd,
    Gazelle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceLocator {
    FilesystemPath(PathBuf),
    ApiClient { client_name: String },
    ManualEntry { submitted_path: PathBuf },
    TrackerRef { tracker: String, identifier: String },
}
