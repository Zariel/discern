use crate::support::ids::{ArtistId, MusicBrainzArtistId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artist {
    pub id: ArtistId,
    pub name: String,
    pub sort_name: Option<String>,
    pub musicbrainz_artist_id: Option<MusicBrainzArtistId>,
}
