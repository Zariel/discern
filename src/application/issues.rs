use crate::application::repository::{
    IssueCommandRepository, IssueRepository, RepositoryError, RepositoryErrorKind,
};
use crate::domain::issue::{Issue, IssueLifecycleError, IssueSubject, IssueType};
use crate::support::ids::IssueId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueServiceError {
    pub kind: IssueServiceErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueServiceErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct IssueService<R> {
    repository: R,
}

impl<R> IssueService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> IssueService<R>
where
    R: IssueRepository + IssueCommandRepository,
{
    pub fn open_issue(
        &self,
        issue_type: IssueType,
        subject: IssueSubject,
        summary: impl Into<String>,
        details: Option<String>,
        created_at_unix_seconds: i64,
    ) -> Result<Issue, IssueServiceError> {
        let issue = Issue::open(
            issue_type,
            subject,
            summary,
            details,
            created_at_unix_seconds,
        );
        self.repository
            .create_issue(&issue)
            .map_err(map_repository_error)?;
        Ok(issue)
    }

    pub fn resolve_issue(
        &self,
        issue_id: &IssueId,
        resolved_at_unix_seconds: i64,
    ) -> Result<Issue, IssueServiceError> {
        let mut issue = self
            .repository
            .get_issue(issue_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| IssueServiceError {
                kind: IssueServiceErrorKind::NotFound,
                message: format!("issue {} was not found", issue_id.as_uuid()),
            })?;
        issue
            .resolve(resolved_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_issue(&issue)
            .map_err(map_repository_error)?;
        Ok(issue)
    }

    pub fn suppress_issue(
        &self,
        issue_id: &IssueId,
        reason: impl Into<String>,
        suppressed_at_unix_seconds: i64,
    ) -> Result<Issue, IssueServiceError> {
        let mut issue = self
            .repository
            .get_issue(issue_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| IssueServiceError {
                kind: IssueServiceErrorKind::NotFound,
                message: format!("issue {} was not found", issue_id.as_uuid()),
            })?;
        issue
            .suppress(reason, suppressed_at_unix_seconds)
            .map_err(map_lifecycle_error)?;
        self.repository
            .update_issue(&issue)
            .map_err(map_repository_error)?;
        Ok(issue)
    }
}

fn map_repository_error(error: RepositoryError) -> IssueServiceError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => IssueServiceErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            IssueServiceErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => IssueServiceErrorKind::Storage,
    };
    IssueServiceError {
        kind,
        message: error.message,
    }
}

fn map_lifecycle_error(error: IssueLifecycleError) -> IssueServiceError {
    IssueServiceError {
        kind: IssueServiceErrorKind::Conflict,
        message: match error {
            IssueLifecycleError::AlreadyResolved => "issue is already resolved".to_string(),
            IssueLifecycleError::AlreadySuppressed => "issue is already suppressed".to_string(),
            IssueLifecycleError::ResolvedIssueCannotBeSuppressed => {
                "resolved issues cannot be suppressed".to_string()
            }
            IssueLifecycleError::SuppressedIssueCannotBeResolved => {
                "suppressed issues cannot be resolved".to_string()
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::application::repository::IssueListQuery;
    use crate::domain::issue::{IssueState, IssueSubject, IssueType};
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn service_creates_and_transitions_issues() {
        let repository = InMemoryIssueRepository::default();
        let service = IssueService::new(repository.clone());

        let issue = service
            .open_issue(
                IssueType::AmbiguousReleaseMatch,
                IssueSubject::Library,
                "Ambiguous match",
                None,
                100,
            )
            .expect("issue creation should succeed");
        assert_eq!(issue.state, IssueState::Open);

        let resolved = service
            .resolve_issue(&issue.id, 120)
            .expect("issue resolution should succeed");
        assert_eq!(resolved.state, IssueState::Resolved);

        let second_issue = service
            .open_issue(
                IssueType::MissingArtwork,
                IssueSubject::Library,
                "Artwork missing",
                None,
                101,
            )
            .expect("issue creation should succeed");
        let suppressed = service
            .suppress_issue(&second_issue.id, "ignored for now", 130)
            .expect("issue suppression should succeed");
        assert_eq!(suppressed.state, IssueState::Suppressed);
        assert_eq!(
            suppressed.suppressed_reason,
            Some("ignored for now".to_string())
        );
    }

    #[test]
    fn service_rejects_invalid_issue_transitions() {
        let repository = InMemoryIssueRepository::default();
        let service = IssueService::new(repository.clone());
        let issue = service
            .open_issue(
                IssueType::DuplicateReleaseInstance,
                IssueSubject::Library,
                "Duplicate release",
                None,
                100,
            )
            .expect("issue creation should succeed");
        service
            .resolve_issue(&issue.id, 120)
            .expect("issue resolution should succeed");

        let error = service
            .suppress_issue(&issue.id, "too late", 130)
            .expect_err("resolved issues should not suppress");
        assert_eq!(error.kind, IssueServiceErrorKind::Conflict);
        assert_eq!(error.message, "resolved issues cannot be suppressed");
    }

    #[derive(Clone, Default)]
    struct InMemoryIssueRepository {
        issues: Arc<Mutex<HashMap<String, Issue>>>,
    }

    impl IssueRepository for InMemoryIssueRepository {
        fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(self
                .issues
                .lock()
                .expect("repository should lock")
                .get(&id.as_uuid().to_string())
                .cloned())
        }

        fn list_issues(&self, _query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: Default::default(),
                total: 0,
            })
        }
    }

    impl IssueCommandRepository for InMemoryIssueRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("repository should lock")
                .insert(issue.id.as_uuid().to_string(), issue.clone());
            Ok(())
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("repository should lock")
                .insert(issue.id.as_uuid().to_string(), issue.clone());
            Ok(())
        }
    }
}
