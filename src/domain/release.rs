use crate::support::ids::{
    ArtistId, DiscogsReleaseId, MusicBrainzReleaseId, ReleaseGroupId, ReleaseId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub id: ReleaseId,
    pub release_group_id: ReleaseGroupId,
    pub primary_artist_id: ArtistId,
    pub title: String,
    pub musicbrainz_release_id: Option<MusicBrainzReleaseId>,
    pub discogs_release_id: Option<DiscogsReleaseId>,
    pub edition: ReleaseEdition,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReleaseEdition {
    pub edition_title: Option<String>,
    pub disambiguation: Option<String>,
    pub country: Option<String>,
    pub label: Option<String>,
    pub catalog_number: Option<String>,
    pub release_date: Option<PartialDate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialDate {
    pub year: u16,
    pub month: Option<u8>,
    pub day: Option<u8>,
}
