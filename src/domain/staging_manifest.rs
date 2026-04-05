use std::path::PathBuf;

use crate::domain::release_instance::FormatFamily;
use crate::domain::source::SourceKind;
use crate::support::ids::{ImportBatchId, StagingManifestId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingManifest {
    pub id: StagingManifestId,
    pub batch_id: ImportBatchId,
    pub source: StagingManifestSource,
    pub discovered_files: Vec<StagedFile>,
    pub auxiliary_files: Vec<AuxiliaryFile>,
    pub grouping: GroupingDecision,
    pub captured_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingManifestSource {
    pub kind: SourceKind,
    pub source_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    pub path: PathBuf,
    pub fingerprint: FileFingerprint,
    pub observed_tags: Vec<ObservedTag>,
    pub duration_ms: Option<u32>,
    pub format_family: FormatFamily,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFingerprint {
    ContentHash(String),
    LightweightFingerprint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryFile {
    pub path: PathBuf,
    pub role: AuxiliaryFileRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuxiliaryFileRole {
    GazelleYaml,
    Artwork,
    CueSheet,
    Log,
    Other { description: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingDecision {
    pub strategy: GroupingStrategy,
    pub groups: Vec<StagedReleaseGroup>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupingStrategy {
    CommonParentDirectory,
    SharedAlbumMetadata,
    TrackNumberContinuity,
    ManualManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedReleaseGroup {
    pub key: String,
    pub file_paths: Vec<PathBuf>,
    pub auxiliary_paths: Vec<PathBuf>,
}
