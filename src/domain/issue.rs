use crate::support::ids::{IssueId, ReleaseId, ReleaseInstanceId, TrackInstanceId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub id: IssueId,
    pub issue_type: IssueType,
    pub state: IssueState,
    pub subject: IssueSubject,
    pub summary: String,
    pub details: Option<String>,
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueType {
    UnmatchedRelease,
    AmbiguousReleaseMatch,
    ConflictingMetadata,
    InconsistentTrackCount,
    MissingTracks,
    CorruptFile,
    UnsupportedFormat,
    DuplicateReleaseInstance,
    UndistinguishableReleaseInstance,
    PlayerVisibilityCollision,
    MissingArtwork,
    BrokenTags,
    MultiDiscAmbiguity,
    CompilationArtistAmbiguity,
    PlayerCompatibilityFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueState {
    Open,
    Resolved,
    Suppressed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueSubject {
    Release(ReleaseId),
    ReleaseInstance(ReleaseInstanceId),
    TrackInstance(TrackInstanceId),
    Library,
}
