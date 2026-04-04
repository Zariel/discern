use crate::support::ids::{ReleaseId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseInstance {
    pub id: ReleaseInstanceId,
    pub release_id: ReleaseId,
    pub state: ReleaseInstanceState,
    pub technical_variant: TechnicalVariant,
    pub provenance: ProvenanceSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseInstanceState {
    Discovered,
    Staged,
    Analyzed,
    Matched,
    NeedsReview,
    RenderingExport,
    Tagging,
    Organizing,
    Imported,
    Verified,
    Quarantined,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TechnicalVariant {
    pub format_family: FormatFamily,
    pub bitrate_mode: BitrateMode,
    pub bitrate_kbps: Option<u32>,
    pub sample_rate_hz: Option<u32>,
    pub bit_depth: Option<u8>,
    pub track_count: u16,
    pub total_duration_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatFamily {
    Flac,
    Mp3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitrateMode {
    Constant,
    Variable,
    Lossless,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenanceSnapshot {
    pub ingest_origin: IngestOrigin,
    pub original_source_path: String,
    pub imported_at_unix_seconds: i64,
    pub gazelle_reference: Option<GazelleReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOrigin {
    WatchDirectory,
    ApiPush,
    ManualAdd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GazelleReference {
    pub tracker: String,
    pub torrent_id: Option<String>,
    pub release_group_id: Option<String>,
}
