use std::path::PathBuf;

use crate::support::ids::{ReleaseArtworkId, ReleaseId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseArtwork {
    pub id: ReleaseArtworkId,
    pub release_id: ReleaseId,
    pub release_instance_id: Option<ReleaseInstanceId>,
    pub source: ArtworkSource,
    pub is_primary: bool,
    pub original_path: Option<PathBuf>,
    pub managed_filename: Option<String>,
    pub mime_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtworkSource {
    OperatorSelected,
    SourceLocal,
    Provider,
}
