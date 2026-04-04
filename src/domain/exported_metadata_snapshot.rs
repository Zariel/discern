use crate::support::ids::{ExportedMetadataSnapshotId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedMetadataSnapshot {
    pub id: ExportedMetadataSnapshotId,
    pub release_instance_id: ReleaseInstanceId,
    pub export_profile: String,
    pub album_title: String,
    pub album_artist: String,
    pub artist_credits: Vec<String>,
    pub edition_visibility: QualifierVisibility,
    pub technical_visibility: QualifierVisibility,
    pub path_components: Vec<String>,
    pub primary_artwork_filename: Option<String>,
    pub compatibility: CompatibilityReport,
    pub rendered_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualifierVisibility {
    Hidden,
    PathOnly,
    TagsAndPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub verified: bool,
    pub warnings: Vec<String>,
}
