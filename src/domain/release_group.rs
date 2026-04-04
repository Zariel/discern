use crate::support::ids::{ArtistId, MusicBrainzReleaseGroupId, ReleaseGroupId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseGroup {
    pub id: ReleaseGroupId,
    pub primary_artist_id: ArtistId,
    pub title: String,
    pub kind: ReleaseGroupKind,
    pub musicbrainz_release_group_id: Option<MusicBrainzReleaseGroupId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseGroupKind {
    Album,
    Ep,
    Single,
    Live,
    Compilation,
    Soundtrack,
    Other(String),
}
