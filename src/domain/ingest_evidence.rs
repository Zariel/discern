use std::path::PathBuf;

use crate::domain::release_instance::FormatFamily;
use crate::support::ids::{ImportBatchId, IngestEvidenceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestEvidenceRecord {
    pub id: IngestEvidenceId,
    pub batch_id: ImportBatchId,
    pub subject: IngestEvidenceSubject,
    pub source: IngestEvidenceSource,
    pub observations: Vec<ObservedValue>,
    pub structured_payload: Option<String>,
    pub captured_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestEvidenceSubject {
    DiscoveredPath(PathBuf),
    GroupedReleaseInput { group_key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestEvidenceSource {
    EmbeddedTags,
    FileName,
    DirectoryStructure,
    GazelleYaml,
    AuxiliaryFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedValue {
    pub kind: ObservedValueKind,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedValueKind {
    Artist,
    ReleaseTitle,
    ReleaseYear,
    TrackTitle,
    TrackNumber,
    DiscNumber,
    DurationMs,
    FormatFamily,
    MediaDescriptor,
    SourceDescriptor,
    TrackerIdentifier,
}

impl ObservedValue {
    pub fn format_family(value: FormatFamily) -> Self {
        Self {
            kind: ObservedValueKind::FormatFamily,
            value: match value {
                FormatFamily::Flac => "flac",
                FormatFamily::Mp3 => "mp3",
            }
            .to_string(),
        }
    }
}
