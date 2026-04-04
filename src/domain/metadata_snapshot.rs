use crate::support::ids::{FileId, MetadataSnapshotId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub id: MetadataSnapshotId,
    pub subject: MetadataSubject,
    pub source: MetadataSnapshotSource,
    pub format: SnapshotFormat,
    pub payload: String,
    pub captured_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataSubject {
    ReleaseInstance(ReleaseInstanceId),
    File(FileId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataSnapshotSource {
    EmbeddedTags,
    FileNameHeuristics,
    DirectoryStructure,
    GazelleYaml,
    MusicBrainzPayload,
    DiscogsPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotFormat {
    Json,
    Yaml,
    Text,
}
