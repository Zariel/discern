use crate::domain::release_instance::FormatFamily;
use crate::domain::track::TrackPosition;
use crate::support::ids::{ReleaseInstanceId, TrackId, TrackInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackInstance {
    pub id: TrackInstanceId,
    pub release_instance_id: ReleaseInstanceId,
    pub track_id: TrackId,
    pub observed_position: TrackPosition,
    pub observed_title: Option<String>,
    pub audio_properties: AudioProperties,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioProperties {
    pub format_family: FormatFamily,
    pub duration_ms: Option<u32>,
    pub bitrate_kbps: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub bit_depth: Option<u8>,
}
