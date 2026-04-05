use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::api::error::{ApiError, ApiErrorCode};
use crate::api::pagination::ApiPaginationMeta;
use crate::application::issues::{IssueService, IssueServiceError, IssueServiceErrorKind};
use crate::application::repository::{
    ExportRepository, IssueListQuery, IssueRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::exported_metadata_snapshot::ExportedMetadataSnapshot;
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::support::ids::IssueId;
use crate::support::pagination::PageRequest;

pub type ApiResult<T> = Result<ApiEnvelope<T>, Box<ApiEnvelope<()>>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListIssuesRequest {
    pub state: Option<IssueStateValue>,
    pub issue_type: Option<IssueTypeValue>,
    pub limit: u32,
    pub offset: u64,
}

impl Default for ListIssuesRequest {
    fn default() -> Self {
        Self {
            state: None,
            issue_type: None,
            limit: PageRequest::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuppressIssueRequest {
    pub reason: String,
    pub suppressed_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueResource {
    pub id: String,
    pub issue_type: IssueTypeValue,
    pub state: IssueStateValue,
    pub subject: IssueSubjectResource,
    pub summary: String,
    pub details: Option<String>,
    pub created_at_unix_seconds: i64,
    pub resolved_at_unix_seconds: Option<i64>,
    pub suppressed_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueDetailResource {
    pub issue: IssueResource,
    pub export_diagnostics: Option<ExportDiagnosticsResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueSubjectResource {
    pub kind: String,
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportDiagnosticsResource {
    pub snapshot_id: String,
    pub export_profile: String,
    pub album_title: String,
    pub path_components: Vec<String>,
    pub compatibility_verified: bool,
    pub compatibility_warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueStateValue {
    Open,
    Resolved,
    Suppressed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueTypeValue {
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

pub struct IssuesApi<R> {
    repository: R,
}

impl<R> IssuesApi<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> IssuesApi<R>
where
    R: Clone
        + IssueRepository
        + crate::application::repository::IssueCommandRepository
        + ExportRepository,
{
    pub fn list_issues(
        &self,
        request_id: impl Into<String>,
        request: ListIssuesRequest,
    ) -> ApiResult<Vec<IssueResource>> {
        let request_id = request_id.into();
        let page = self
            .repository
            .list_issues(&IssueListQuery {
                state: request.state.map(Into::into),
                issue_type: request.issue_type.map(Into::into),
                subject: None,
                page: PageRequest::new(request.limit, request.offset),
            })
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success_with_pagination(
            page.items.iter().map(IssueResource::from).collect(),
            request_id,
            ApiPaginationMeta::from_page(&page),
        ))
    }

    pub fn get_issue(
        &self,
        request_id: impl Into<String>,
        issue_id: &str,
    ) -> ApiResult<IssueDetailResource> {
        let request_id = request_id.into();
        let issue_id = parse_issue_id(issue_id, &request_id)?;
        let issue = self
            .repository
            .get_issue(&issue_id)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?
            .ok_or_else(|| {
                not_found_envelope("issue", issue_id.as_uuid().to_string(), &request_id)
            })?;
        let export_diagnostics = export_diagnostics_for_issue(&self.repository, &issue)
            .map_err(|error| repository_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            IssueDetailResource {
                issue: IssueResource::from(&issue),
                export_diagnostics,
            },
            request_id,
        ))
    }

    pub fn resolve_issue(
        &self,
        request_id: impl Into<String>,
        issue_id: &str,
        resolved_at_unix_seconds: i64,
    ) -> ApiResult<IssueResource> {
        let request_id = request_id.into();
        let issue_id = parse_issue_id(issue_id, &request_id)?;
        let issue = IssueService::new(self.repository.clone())
            .resolve_issue(&issue_id, resolved_at_unix_seconds)
            .map_err(|error| issue_service_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            IssueResource::from(&issue),
            request_id,
        ))
    }

    pub fn suppress_issue(
        &self,
        request_id: impl Into<String>,
        issue_id: &str,
        request: SuppressIssueRequest,
    ) -> ApiResult<IssueResource> {
        let request_id = request_id.into();
        let issue_id = parse_issue_id(issue_id, &request_id)?;
        let issue = IssueService::new(self.repository.clone())
            .suppress_issue(
                &issue_id,
                request.reason,
                request.suppressed_at_unix_seconds,
            )
            .map_err(|error| issue_service_error_envelope(error, request_id.clone()))?;
        Ok(ApiEnvelope::success(
            IssueResource::from(&issue),
            request_id,
        ))
    }
}

impl From<&Issue> for IssueResource {
    fn from(value: &Issue) -> Self {
        Self {
            id: value.id.as_uuid().to_string(),
            issue_type: value.issue_type.clone().into(),
            state: value.state.clone().into(),
            subject: IssueSubjectResource::from(&value.subject),
            summary: value.summary.clone(),
            details: value.details.clone(),
            created_at_unix_seconds: value.created_at_unix_seconds,
            resolved_at_unix_seconds: value.resolved_at_unix_seconds,
            suppressed_reason: value.suppressed_reason.clone(),
        }
    }
}

impl From<&IssueSubject> for IssueSubjectResource {
    fn from(value: &IssueSubject) -> Self {
        match value {
            IssueSubject::Release(id) => Self {
                kind: "release".to_string(),
                id: Some(id.as_uuid().to_string()),
            },
            IssueSubject::ReleaseInstance(id) => Self {
                kind: "release_instance".to_string(),
                id: Some(id.as_uuid().to_string()),
            },
            IssueSubject::TrackInstance(id) => Self {
                kind: "track_instance".to_string(),
                id: Some(id.as_uuid().to_string()),
            },
            IssueSubject::Library => Self {
                kind: "library".to_string(),
                id: None,
            },
        }
    }
}

impl From<&ExportedMetadataSnapshot> for ExportDiagnosticsResource {
    fn from(value: &ExportedMetadataSnapshot) -> Self {
        Self {
            snapshot_id: value.id.as_uuid().to_string(),
            export_profile: value.export_profile.clone(),
            album_title: value.album_title.clone(),
            path_components: value.path_components.clone(),
            compatibility_verified: value.compatibility.verified,
            compatibility_warnings: value.compatibility.warnings.clone(),
        }
    }
}

impl From<IssueState> for IssueStateValue {
    fn from(value: IssueState) -> Self {
        match value {
            IssueState::Open => Self::Open,
            IssueState::Resolved => Self::Resolved,
            IssueState::Suppressed => Self::Suppressed,
        }
    }
}

impl From<IssueStateValue> for IssueState {
    fn from(value: IssueStateValue) -> Self {
        match value {
            IssueStateValue::Open => Self::Open,
            IssueStateValue::Resolved => Self::Resolved,
            IssueStateValue::Suppressed => Self::Suppressed,
        }
    }
}

impl From<IssueType> for IssueTypeValue {
    fn from(value: IssueType) -> Self {
        match value {
            IssueType::UnmatchedRelease => Self::UnmatchedRelease,
            IssueType::AmbiguousReleaseMatch => Self::AmbiguousReleaseMatch,
            IssueType::ConflictingMetadata => Self::ConflictingMetadata,
            IssueType::InconsistentTrackCount => Self::InconsistentTrackCount,
            IssueType::MissingTracks => Self::MissingTracks,
            IssueType::CorruptFile => Self::CorruptFile,
            IssueType::UnsupportedFormat => Self::UnsupportedFormat,
            IssueType::DuplicateReleaseInstance => Self::DuplicateReleaseInstance,
            IssueType::UndistinguishableReleaseInstance => Self::UndistinguishableReleaseInstance,
            IssueType::PlayerVisibilityCollision => Self::PlayerVisibilityCollision,
            IssueType::MissingArtwork => Self::MissingArtwork,
            IssueType::BrokenTags => Self::BrokenTags,
            IssueType::MultiDiscAmbiguity => Self::MultiDiscAmbiguity,
            IssueType::CompilationArtistAmbiguity => Self::CompilationArtistAmbiguity,
            IssueType::PlayerCompatibilityFailure => Self::PlayerCompatibilityFailure,
        }
    }
}

impl From<IssueTypeValue> for IssueType {
    fn from(value: IssueTypeValue) -> Self {
        match value {
            IssueTypeValue::UnmatchedRelease => Self::UnmatchedRelease,
            IssueTypeValue::AmbiguousReleaseMatch => Self::AmbiguousReleaseMatch,
            IssueTypeValue::ConflictingMetadata => Self::ConflictingMetadata,
            IssueTypeValue::InconsistentTrackCount => Self::InconsistentTrackCount,
            IssueTypeValue::MissingTracks => Self::MissingTracks,
            IssueTypeValue::CorruptFile => Self::CorruptFile,
            IssueTypeValue::UnsupportedFormat => Self::UnsupportedFormat,
            IssueTypeValue::DuplicateReleaseInstance => Self::DuplicateReleaseInstance,
            IssueTypeValue::UndistinguishableReleaseInstance => {
                Self::UndistinguishableReleaseInstance
            }
            IssueTypeValue::PlayerVisibilityCollision => Self::PlayerVisibilityCollision,
            IssueTypeValue::MissingArtwork => Self::MissingArtwork,
            IssueTypeValue::BrokenTags => Self::BrokenTags,
            IssueTypeValue::MultiDiscAmbiguity => Self::MultiDiscAmbiguity,
            IssueTypeValue::CompilationArtistAmbiguity => Self::CompilationArtistAmbiguity,
            IssueTypeValue::PlayerCompatibilityFailure => Self::PlayerCompatibilityFailure,
        }
    }
}

fn export_diagnostics_for_issue<R>(
    repository: &R,
    issue: &Issue,
) -> Result<Option<ExportDiagnosticsResource>, RepositoryError>
where
    R: ExportRepository,
{
    let IssueSubject::ReleaseInstance(release_instance_id) = &issue.subject else {
        return Ok(None);
    };
    Ok(repository
        .get_latest_exported_metadata(release_instance_id)?
        .as_ref()
        .map(ExportDiagnosticsResource::from))
}

fn parse_issue_id(issue_id: &str, request_id: &str) -> Result<IssueId, Box<ApiEnvelope<()>>> {
    IssueId::parse_str(issue_id).map_err(|_| invalid_id_envelope("issue", issue_id, request_id))
}

fn invalid_id_envelope(kind: &str, value: &str, request_id: &str) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            ApiErrorCode::InvalidRequest,
            format!("{kind} id '{value}' is not a valid UUID"),
            None,
        ),
        request_id.to_string(),
    ))
}

fn not_found_envelope(kind: &str, id: String, request_id: &str) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            ApiErrorCode::NotFound,
            format!("{kind} {id} was not found"),
            None,
        ),
        request_id.to_string(),
    ))
}

fn repository_error_envelope(error: RepositoryError, request_id: String) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                RepositoryErrorKind::NotFound => ApiErrorCode::NotFound,
                RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
                    ApiErrorCode::Conflict
                }
                RepositoryErrorKind::Storage => ApiErrorCode::InternalError,
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

fn issue_service_error_envelope(
    error: IssueServiceError,
    request_id: String,
) -> Box<ApiEnvelope<()>> {
    Box::new(ApiEnvelope::error(
        ApiError::new(
            match error.kind {
                IssueServiceErrorKind::NotFound => ApiErrorCode::NotFound,
                IssueServiceErrorKind::Conflict => ApiErrorCode::Conflict,
                IssueServiceErrorKind::Storage => ApiErrorCode::InternalError,
            },
            error.message,
            None,
        ),
        request_id,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::domain::exported_metadata_snapshot::{
        CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
    };
    use std::collections::HashMap;
    use crate::domain::issue::{Issue, IssueSubject, IssueType};
    use crate::support::ids::{ExportedMetadataSnapshotId, ReleaseInstanceId};
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn list_issues_returns_paginated_resources() {
        let repository = InMemoryIssuesRepository::seeded();
        let api = IssuesApi::new(repository);

        let envelope = api
            .list_issues(
                "req_issues",
                ListIssuesRequest {
                    limit: 1,
                    offset: 0,
                    ..ListIssuesRequest::default()
                },
            )
            .expect("issue list should succeed");

        let data = envelope.data.expect("data should exist");
        assert_eq!(data.len(), 1);
        assert_eq!(
            envelope
                .meta
                .pagination
                .expect("pagination should exist")
                .next_offset,
            Some(1)
        );
    }

    #[test]
    fn get_issue_returns_export_diagnostics_for_release_instance_subject() {
        let repository = InMemoryIssuesRepository::seeded();
        let issue_id = repository.primary_issue_id();
        let api = IssuesApi::new(repository);

        let envelope = api
            .get_issue("req_issue", &issue_id)
            .expect("issue detail should succeed");

        let detail = envelope.data.expect("data should exist");
        let diagnostics = detail
            .export_diagnostics
            .expect("release-instance issue should include export diagnostics");
        assert_eq!(diagnostics.export_profile, "generic_player");
        assert_eq!(diagnostics.compatibility_warnings.len(), 1);
    }

    #[test]
    fn resolve_issue_updates_state() {
        let repository = InMemoryIssuesRepository::seeded();
        let issue_id = repository.primary_issue_id();
        let api = IssuesApi::new(repository);

        let envelope = api
            .resolve_issue("req_resolve", &issue_id, 200)
            .expect("issue resolution should succeed");

        let issue = envelope.data.expect("data should exist");
        assert_eq!(issue.state, IssueStateValue::Resolved);
    }

    #[test]
    fn suppress_issue_updates_state_and_reason() {
        let repository = InMemoryIssuesRepository::seeded();
        let issue_id = repository.secondary_issue_id();
        let api = IssuesApi::new(repository);

        let envelope = api
            .suppress_issue(
                "req_suppress",
                &issue_id,
                SuppressIssueRequest {
                    reason: "operator accepted risk".to_string(),
                    suppressed_at_unix_seconds: 220,
                },
            )
            .expect("issue suppression should succeed");

        let issue = envelope.data.expect("data should exist");
        assert_eq!(issue.state, IssueStateValue::Suppressed);
        assert_eq!(
            issue.suppressed_reason,
            Some("operator accepted risk".to_string())
        );
    }

    #[derive(Clone)]
    struct InMemoryIssuesRepository {
        issues: Arc<Mutex<HashMap<String, Issue>>>,
        snapshot: ExportedMetadataSnapshot,
    }

    impl InMemoryIssuesRepository {
        fn seeded() -> Self {
            let release_instance_id = ReleaseInstanceId::new();
            let primary_issue = Issue::open(
                IssueType::PlayerCompatibilityFailure,
                IssueSubject::ReleaseInstance(release_instance_id.clone()),
                "Compatibility warning",
                Some("Managed files were not distinct".to_string()),
                100,
            );
            let secondary_issue = Issue::open(
                IssueType::MissingArtwork,
                IssueSubject::Library,
                "Artwork missing",
                None,
                110,
            );

            Self {
                issues: Arc::new(Mutex::new(HashMap::from([
                    (primary_issue.id.as_uuid().to_string(), primary_issue),
                    (secondary_issue.id.as_uuid().to_string(), secondary_issue),
                ]))),
                snapshot: ExportedMetadataSnapshot {
                    id: ExportedMetadataSnapshotId::new(),
                    release_instance_id,
                    export_profile: "generic_player".to_string(),
                    album_title: "Kid A [2000]".to_string(),
                    album_artist: "Radiohead".to_string(),
                    artist_credits: vec!["Radiohead".to_string()],
                    edition_visibility: QualifierVisibility::TagsAndPath,
                    technical_visibility: QualifierVisibility::PathOnly,
                    path_components: vec!["Radiohead".to_string(), "Kid A [2000]".to_string()],
                    primary_artwork_filename: Some("cover.jpg".to_string()),
                    compatibility: CompatibilityReport {
                        verified: false,
                        warnings: vec!["path collision with sibling edition".to_string()],
                    },
                    rendered_at_unix_seconds: 120,
                },
            }
        }

        fn primary_issue_id(&self) -> String {
            self.issues
                .lock()
                .expect("issues should lock")
                .values()
                .find(|issue| matches!(issue.subject, IssueSubject::ReleaseInstance(_)))
                .expect("primary issue should exist")
                .id
                .as_uuid()
                .to_string()
        }

        fn secondary_issue_id(&self) -> String {
            self.issues
                .lock()
                .expect("issues should lock")
                .values()
                .find(|issue| matches!(issue.subject, IssueSubject::Library))
                .expect("secondary issue should exist")
                .id
                .as_uuid()
                .to_string()
        }
    }

    impl IssueRepository for InMemoryIssuesRepository {
        fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(self
                .issues
                .lock()
                .expect("issues should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            let mut items = self
                .issues
                .lock()
                .expect("issues should lock")
                .values()
                .filter(|issue| {
                    query
                        .state
                        .as_ref()
                        .is_none_or(|state| &issue.state == state)
                })
                .filter(|issue| {
                    query
                        .issue_type
                        .as_ref()
                        .is_none_or(|issue_type| &issue.issue_type == issue_type)
                })
                .filter(|issue| {
                    query
                        .subject
                        .as_ref()
                        .is_none_or(|subject| &issue.subject == subject)
                })
                .cloned()
                .collect::<Vec<_>>();
            items.sort_by_key(|issue| issue.created_at_unix_seconds);
            let total = items.len() as u64;
            items = items
                .into_iter()
                .skip(query.page.offset as usize)
                .take(query.page.limit as usize)
                .collect();
            Ok(Page {
                items,
                request: query.page,
                total,
            })
        }
    }

    impl crate::application::repository::IssueCommandRepository for InMemoryIssuesRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .insert(issue.id.as_uuid().to_string(), issue.clone());
            Ok(())
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .insert(issue.id.as_uuid().to_string(), issue.clone());
            Ok(())
        }
    }

    impl ExportRepository for InMemoryIssuesRepository {
        fn get_latest_exported_metadata(
            &self,
            release_instance_id: &ReleaseInstanceId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            if &self.snapshot.release_instance_id == release_instance_id {
                Ok(Some(self.snapshot.clone()))
            } else {
                Ok(None)
            }
        }

        fn list_exported_metadata(
            &self,
            _query: &crate::application::repository::ExportedMetadataListQuery,
        ) -> Result<Page<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(Page {
                items: vec![self.snapshot.clone()],
                request: PageRequest::default(),
                total: 1,
            })
        }

        fn get_exported_metadata(
            &self,
            _id: &ExportedMetadataSnapshotId,
        ) -> Result<Option<ExportedMetadataSnapshot>, RepositoryError> {
            Ok(Some(self.snapshot.clone()))
        }
    }
}
