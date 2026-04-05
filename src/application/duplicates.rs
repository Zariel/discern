use crate::application::config::ImportPolicy;
use crate::application::repository::{
    IssueCommandRepository, IssueListQuery, IssueRepository, ReleaseInstanceListQuery,
    ReleaseInstanceRepository, RepositoryError, RepositoryErrorKind,
};
use crate::config::DuplicatePolicy;
use crate::domain::issue::{Issue, IssueLifecycleError, IssueState, IssueSubject, IssueType};
use crate::domain::release_instance::{ReleaseInstance, ReleaseInstanceState};
use crate::support::ids::ReleaseInstanceId;
use crate::support::pagination::PageRequest;

const DUPLICATE_DURATION_TOLERANCE_SECONDS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateHandlingReport {
    pub duplicates: Vec<ReleaseInstance>,
    pub quarantined: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateHandlingError {
    pub kind: DuplicateHandlingErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuplicateHandlingErrorKind {
    NotFound,
    Conflict,
    Storage,
}

pub struct DuplicateHandlingService<R> {
    repository: R,
}

impl<R> DuplicateHandlingService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> DuplicateHandlingService<R>
where
    R: IssueCommandRepository + IssueRepository + ReleaseInstanceRepository,
{
    pub fn evaluate_release_instance(
        &self,
        import_policy: &ImportPolicy,
        release_instance_id: &ReleaseInstanceId,
        changed_at_unix_seconds: i64,
    ) -> Result<DuplicateHandlingReport, DuplicateHandlingError> {
        let release_instance = self
            .repository
            .get_release_instance(release_instance_id)
            .map_err(map_repository_error)?
            .ok_or_else(|| DuplicateHandlingError {
                kind: DuplicateHandlingErrorKind::NotFound,
                message: format!(
                    "no release instance found for {}",
                    release_instance_id.as_uuid()
                ),
            })?;

        let duplicates = find_duplicates(&self.repository, &release_instance)?;
        synchronize_duplicate_issue(
            &self.repository,
            import_policy,
            &release_instance,
            &duplicates,
            changed_at_unix_seconds,
        )?;

        Ok(DuplicateHandlingReport {
            quarantined: !duplicates.is_empty()
                && import_policy.duplicate_policy == DuplicatePolicy::Quarantine,
            duplicates,
        })
    }
}

fn find_duplicates<R>(
    repository: &R,
    release_instance: &ReleaseInstance,
) -> Result<Vec<ReleaseInstance>, DuplicateHandlingError>
where
    R: ReleaseInstanceRepository,
{
    let Some(release_id) = release_instance.release_id.clone() else {
        return Ok(Vec::new());
    };

    let candidates = repository
        .list_release_instances(&ReleaseInstanceListQuery {
            release_id: Some(release_id),
            state: None,
            format_family: Some(release_instance.technical_variant.format_family.clone()),
            page: PageRequest::new(100, 0),
        })
        .map_err(map_repository_error)?;

    Ok(candidates
        .items
        .into_iter()
        .filter(|candidate| candidate.id != release_instance.id)
        .filter(|candidate| candidate.source_id != release_instance.source_id)
        .filter(|candidate| candidate.release_id == release_instance.release_id)
        .filter(|candidate| !matches!(candidate.state, ReleaseInstanceState::Failed))
        .filter(|candidate| {
            candidate.technical_variant.track_count
                == release_instance.technical_variant.track_count
        })
        .filter(|candidate| {
            candidate.technical_variant.bitrate_mode
                == release_instance.technical_variant.bitrate_mode
        })
        .filter(|candidate| {
            candidate
                .technical_variant
                .total_duration_seconds
                .abs_diff(release_instance.technical_variant.total_duration_seconds)
                <= DUPLICATE_DURATION_TOLERANCE_SECONDS
        })
        .collect())
}

fn synchronize_duplicate_issue<R>(
    repository: &R,
    import_policy: &ImportPolicy,
    release_instance: &ReleaseInstance,
    duplicates: &[ReleaseInstance],
    changed_at_unix_seconds: i64,
) -> Result<(), DuplicateHandlingError>
where
    R: IssueCommandRepository + IssueRepository,
{
    let subject = IssueSubject::ReleaseInstance(release_instance.id.clone());
    let existing = repository
        .list_issues(&IssueListQuery {
            state: Some(IssueState::Open),
            issue_type: Some(IssueType::DuplicateReleaseInstance),
            subject: Some(subject.clone()),
            page: PageRequest::new(50, 0),
        })
        .map_err(map_repository_error)?;

    let should_open = !duplicates.is_empty()
        && import_policy.duplicate_policy != DuplicatePolicy::AllowIfDistinguishable;

    if should_open {
        let summary = format!(
            "Duplicate import candidates found for {}",
            release_instance.id.as_uuid()
        );
        let details = render_duplicate_details(duplicates);
        if let Some(mut issue) = existing.items.into_iter().next() {
            if issue.summary != summary || issue.details != Some(details.clone()) {
                issue.summary = summary;
                issue.details = Some(details);
                repository
                    .update_issue(&issue)
                    .map_err(map_repository_error)?;
            }
        } else {
            repository
                .create_issue(&Issue::open(
                    IssueType::DuplicateReleaseInstance,
                    subject,
                    summary,
                    Some(details),
                    changed_at_unix_seconds,
                ))
                .map_err(map_repository_error)?;
        }
    } else {
        for mut issue in existing.items {
            issue
                .resolve(changed_at_unix_seconds)
                .map_err(map_issue_lifecycle_error)?;
            repository
                .update_issue(&issue)
                .map_err(map_repository_error)?;
        }
    }

    Ok(())
}

fn render_duplicate_details(duplicates: &[ReleaseInstance]) -> String {
    let mut lines = vec!["Matched release already exists from another source:".to_string()];
    for duplicate in duplicates {
        lines.push(format!(
            "- {} source={} state={:?} duration={}s",
            duplicate.id.as_uuid(),
            duplicate.source_id.as_uuid(),
            duplicate.state,
            duplicate.technical_variant.total_duration_seconds,
        ));
    }
    lines.join("\n")
}

fn map_repository_error(error: RepositoryError) -> DuplicateHandlingError {
    let kind = match error.kind {
        RepositoryErrorKind::NotFound => DuplicateHandlingErrorKind::NotFound,
        RepositoryErrorKind::Conflict | RepositoryErrorKind::InvalidQuery => {
            DuplicateHandlingErrorKind::Conflict
        }
        RepositoryErrorKind::Storage => DuplicateHandlingErrorKind::Storage,
    };
    DuplicateHandlingError {
        kind,
        message: error.message,
    }
}

fn map_issue_lifecycle_error(error: IssueLifecycleError) -> DuplicateHandlingError {
    DuplicateHandlingError {
        kind: DuplicateHandlingErrorKind::Conflict,
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
    use std::sync::{Arc, Mutex};

    use crate::application::config::ValidatedRuntimeConfig;
    use crate::config::{AppConfig, DuplicatePolicy, ImportConfig};
    use crate::domain::issue::Issue;
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, TechnicalVariant,
    };
    use crate::support::ids::{ImportBatchId, IssueId, ReleaseId, SourceId};
    use crate::support::pagination::Page;

    use super::*;

    #[test]
    fn service_flags_duplicate_release_instances() {
        let repository = InMemoryDuplicateRepository::new(DuplicatePolicy::Flag);
        let service = DuplicateHandlingService::new(repository.clone());

        let report = service
            .evaluate_release_instance(&repository.import_policy(), &repository.primary_id, 200)
            .expect("duplicate evaluation should succeed");

        assert_eq!(report.duplicates.len(), 1);
        assert!(!report.quarantined);
        assert!(repository.has_open_duplicate_issue());
    }

    #[test]
    fn service_quarantines_duplicate_release_instances() {
        let repository = InMemoryDuplicateRepository::new(DuplicatePolicy::Quarantine);
        let service = DuplicateHandlingService::new(repository.clone());

        let report = service
            .evaluate_release_instance(&repository.import_policy(), &repository.primary_id, 200)
            .expect("duplicate evaluation should succeed");

        assert_eq!(report.duplicates.len(), 1);
        assert!(report.quarantined);
        assert!(repository.has_open_duplicate_issue());
    }

    #[test]
    fn allow_policy_resolves_existing_duplicate_issue() {
        let repository = InMemoryDuplicateRepository::new(DuplicatePolicy::AllowIfDistinguishable);
        repository.seed_open_duplicate_issue();
        let service = DuplicateHandlingService::new(repository.clone());

        let report = service
            .evaluate_release_instance(&repository.import_policy(), &repository.primary_id, 200)
            .expect("duplicate evaluation should succeed");

        assert_eq!(report.duplicates.len(), 1);
        assert!(!report.quarantined);
        assert!(!repository.has_open_duplicate_issue());
    }

    #[derive(Clone)]
    struct InMemoryDuplicateRepository {
        release_instances: Arc<Mutex<Vec<ReleaseInstance>>>,
        issues: Arc<Mutex<Vec<Issue>>>,
        primary_id: ReleaseInstanceId,
        import_policy: ImportPolicy,
    }

    impl InMemoryDuplicateRepository {
        fn new(duplicate_policy: DuplicatePolicy) -> Self {
            let release_id = ReleaseId::new();
            let batch_id = ImportBatchId::new();
            let primary_source_id = SourceId::new();
            let duplicate_source_id = SourceId::new();
            let primary_id = ReleaseInstanceId::new();
            let duplicate_id = ReleaseInstanceId::new();
            let release_instances = vec![
                seeded_release_instance(
                    primary_id.clone(),
                    batch_id.clone(),
                    primary_source_id,
                    release_id.clone(),
                    ReleaseInstanceState::Matched,
                    250,
                ),
                seeded_release_instance(
                    duplicate_id,
                    batch_id,
                    duplicate_source_id,
                    release_id,
                    ReleaseInstanceState::Verified,
                    254,
                ),
            ];
            let config = ValidatedRuntimeConfig::from_validated_app_config(&AppConfig {
                import: ImportConfig {
                    duplicate_policy: duplicate_policy.clone(),
                    ..ImportConfig::default()
                },
                ..AppConfig::default()
            });

            Self {
                release_instances: Arc::new(Mutex::new(release_instances)),
                issues: Arc::new(Mutex::new(Vec::new())),
                primary_id,
                import_policy: config.import,
            }
        }

        fn import_policy(&self) -> ImportPolicy {
            self.import_policy.clone()
        }

        fn seed_open_duplicate_issue(&self) {
            self.issues
                .lock()
                .expect("issues should lock")
                .push(Issue::open(
                    IssueType::DuplicateReleaseInstance,
                    IssueSubject::ReleaseInstance(self.primary_id.clone()),
                    "Duplicate import candidates found".to_string(),
                    Some("stale duplicate".to_string()),
                    100,
                ));
        }

        fn has_open_duplicate_issue(&self) -> bool {
            self.issues
                .lock()
                .expect("issues should lock")
                .iter()
                .any(|issue| {
                    issue.state == IssueState::Open
                        && issue.issue_type == IssueType::DuplicateReleaseInstance
                })
        }
    }

    impl ReleaseInstanceRepository for InMemoryDuplicateRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .lock()
                .expect("release instances should lock")
                .iter()
                .find(|candidate| candidate.id == *id)
                .cloned())
        }

        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            let items =
                self.release_instances
                    .lock()
                    .expect("release instances should lock")
                    .iter()
                    .filter(|candidate| {
                        query.release_id.as_ref().is_none_or(|release_id| {
                            candidate.release_id.as_ref() == Some(release_id)
                        })
                    })
                    .filter(|candidate| {
                        query.format_family.as_ref().is_none_or(|format_family| {
                            &candidate.technical_variant.format_family == format_family
                        })
                    })
                    .filter(|candidate| {
                        query
                            .state
                            .as_ref()
                            .is_none_or(|state| &candidate.state == state)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }

        fn list_release_instances_for_batch(
            &self,
            _import_batch_id: &ImportBatchId,
        ) -> Result<Vec<ReleaseInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_candidate_matches(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _page: &PageRequest,
        ) -> Result<Page<crate::domain::candidate_match::CandidateMatch>, RepositoryError> {
            Ok(Page {
                items: Vec::new(),
                request: PageRequest::new(50, 0),
                total: 0,
            })
        }

        fn get_candidate_match(
            &self,
            _id: &crate::support::ids::CandidateMatchId,
        ) -> Result<Option<crate::domain::candidate_match::CandidateMatch>, RepositoryError>
        {
            Ok(None)
        }

        fn list_track_instances_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
        ) -> Result<Vec<crate::domain::track_instance::TrackInstance>, RepositoryError> {
            Ok(Vec::new())
        }

        fn list_files_for_release_instance(
            &self,
            _release_instance_id: &ReleaseInstanceId,
            _role: Option<crate::domain::file::FileRole>,
        ) -> Result<Vec<crate::domain::file::FileRecord>, RepositoryError> {
            Ok(Vec::new())
        }
    }

    impl IssueRepository for InMemoryDuplicateRepository {
        fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
                .find(|issue| issue.id == *id)
                .cloned())
        }

        fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            let items = self
                .issues
                .lock()
                .expect("issues should lock")
                .iter()
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
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl IssueCommandRepository for InMemoryDuplicateRepository {
        fn create_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            self.issues
                .lock()
                .expect("issues should lock")
                .push(issue.clone());
            Ok(())
        }

        fn update_issue(&self, issue: &Issue) -> Result<(), RepositoryError> {
            let mut issues = self.issues.lock().expect("issues should lock");
            if let Some(stored) = issues.iter_mut().find(|stored| stored.id == issue.id) {
                *stored = issue.clone();
            }
            Ok(())
        }
    }

    fn seeded_release_instance(
        id: ReleaseInstanceId,
        import_batch_id: ImportBatchId,
        source_id: SourceId,
        release_id: ReleaseId,
        state: ReleaseInstanceState,
        duration_seconds: u32,
    ) -> ReleaseInstance {
        ReleaseInstance {
            id,
            import_batch_id,
            source_id,
            release_id: Some(release_id),
            state,
            technical_variant: TechnicalVariant {
                format_family: FormatFamily::Mp3,
                bitrate_mode: BitrateMode::Variable,
                bitrate_kbps: Some(320),
                sample_rate_hz: Some(44_100),
                bit_depth: None,
                track_count: 10,
                total_duration_seconds: duration_seconds,
            },
            provenance: ProvenanceSnapshot {
                ingest_origin: IngestOrigin::ManualAdd,
                original_source_path: "/tmp/source".to_string(),
                imported_at_unix_seconds: 1,
                gazelle_reference: None,
            },
        }
    }
}
