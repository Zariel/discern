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
    pub resolved_at_unix_seconds: Option<i64>,
    pub suppressed_reason: Option<String>,
}

impl Issue {
    pub fn open(
        issue_type: IssueType,
        subject: IssueSubject,
        summary: impl Into<String>,
        details: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Self {
        Self {
            id: IssueId::new(),
            issue_type,
            state: IssueState::Open,
            subject,
            summary: summary.into(),
            details,
            created_at_unix_seconds,
            resolved_at_unix_seconds: None,
            suppressed_reason: None,
        }
    }

    pub fn resolve(&mut self, resolved_at_unix_seconds: i64) -> Result<(), IssueLifecycleError> {
        match self.state {
            IssueState::Open => {
                self.state = IssueState::Resolved;
                self.resolved_at_unix_seconds = Some(resolved_at_unix_seconds);
                self.suppressed_reason = None;
                Ok(())
            }
            IssueState::Resolved => Err(IssueLifecycleError::AlreadyResolved),
            IssueState::Suppressed => Err(IssueLifecycleError::SuppressedIssueCannotBeResolved),
        }
    }

    pub fn suppress(
        &mut self,
        reason: impl Into<String>,
        suppressed_at_unix_seconds: i64,
    ) -> Result<(), IssueLifecycleError> {
        match self.state {
            IssueState::Open => {
                self.state = IssueState::Suppressed;
                self.resolved_at_unix_seconds = Some(suppressed_at_unix_seconds);
                self.suppressed_reason = Some(reason.into());
                Ok(())
            }
            IssueState::Resolved => Err(IssueLifecycleError::ResolvedIssueCannotBeSuppressed),
            IssueState::Suppressed => Err(IssueLifecycleError::AlreadySuppressed),
        }
    }
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
pub enum IssueLifecycleError {
    AlreadyResolved,
    AlreadySuppressed,
    ResolvedIssueCannotBeSuppressed,
    SuppressedIssueCannotBeResolved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueSubject {
    Release(ReleaseId),
    ReleaseInstance(ReleaseInstanceId),
    TrackInstance(TrackInstanceId),
    Library,
}
