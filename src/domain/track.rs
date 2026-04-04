use crate::support::ids::{MusicBrainzTrackId, ReleaseId, TrackId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Track {
    pub id: TrackId,
    pub release_id: ReleaseId,
    pub position: TrackPosition,
    pub title: String,
    pub musicbrainz_track_id: Option<MusicBrainzTrackId>,
    pub duration_ms: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackPosition {
    pub disc_number: u16,
    pub track_number: u16,
}
