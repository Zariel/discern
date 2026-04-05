use crate::support::ids::{ManualOverrideId, ReleaseId, ReleaseInstanceId, TrackId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualOverride {
    pub id: ManualOverrideId,
    pub subject: OverrideSubject,
    pub field: OverrideField,
    pub value: String,
    pub note: Option<String>,
    pub created_by: String,
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideSubject {
    Release(ReleaseId),
    ReleaseInstance(ReleaseInstanceId),
    Track(TrackId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideField {
    ReleaseMatch,
    Title,
    AlbumArtist,
    ArtistCredit,
    TrackTitle,
    ReleaseDate,
    EditionQualifier,
    ArtworkSelection,
}
