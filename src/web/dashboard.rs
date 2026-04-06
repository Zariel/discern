use crate::application::repository::{
    ImportBatchListQuery, ImportBatchRepository, IssueListQuery, IssueRepository, JobListQuery,
    JobRepository, ReleaseInstanceListQuery, ReleaseInstanceRepository,
};
use crate::domain::issue::{IssueState, IssueType};
use crate::domain::job::JobStatus;
use crate::domain::release_instance::ReleaseInstanceState;
use crate::support::pagination::PageRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationsDashboard {
    pub total_import_batches: u64,
    pub open_issues: u64,
    pub duplicate_issues: u64,
    pub failed_jobs: u64,
    pub compatibility_regressions: u64,
    pub needs_review_release_instances: u64,
    pub queries: Vec<DashboardQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardQuery {
    pub name: String,
    pub description: String,
    pub promql: String,
}

pub struct OperationsDashboardLoader<R> {
    repository: R,
}

impl<R> OperationsDashboardLoader<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

impl<R> OperationsDashboardLoader<R>
where
    R: ImportBatchRepository + IssueRepository + JobRepository + ReleaseInstanceRepository,
{
    pub fn load(&self) -> Result<OperationsDashboard, String> {
        let total_import_batches = self
            .repository
            .list_import_batches(&ImportBatchListQuery {
                page: PageRequest::new(PageRequest::MAX_LIMIT, 0),
            })
            .map_err(|error| error.message.clone())?
            .total;
        let open_issues = self
            .repository
            .list_issues(&IssueListQuery {
                state: Some(IssueState::Open),
                ..IssueListQuery::default()
            })
            .map_err(|error| error.message.clone())?
            .total;
        let duplicate_issues = self
            .repository
            .list_issues(&IssueListQuery {
                state: Some(IssueState::Open),
                issue_type: Some(IssueType::DuplicateReleaseInstance),
                ..IssueListQuery::default()
            })
            .map_err(|error| error.message.clone())?
            .total;
        let failed_jobs = self
            .repository
            .list_jobs(&JobListQuery {
                status: Some(JobStatus::Failed),
                ..JobListQuery::default()
            })
            .map_err(|error| error.message.clone())?
            .total;
        let compatibility_regressions = self
            .repository
            .list_issues(&IssueListQuery {
                state: Some(IssueState::Open),
                issue_type: Some(IssueType::PlayerCompatibilityFailure),
                ..IssueListQuery::default()
            })
            .map_err(|error| error.message.clone())?
            .total
            + self
                .repository
                .list_issues(&IssueListQuery {
                    state: Some(IssueState::Open),
                    issue_type: Some(IssueType::PlayerVisibilityCollision),
                    ..IssueListQuery::default()
                })
                .map_err(|error| error.message.clone())?
                .total;
        let needs_review_release_instances = self
            .repository
            .list_release_instances(&ReleaseInstanceListQuery {
                state: Some(ReleaseInstanceState::NeedsReview),
                ..ReleaseInstanceListQuery::default()
            })
            .map_err(|error| error.message.clone())?
            .total;

        Ok(OperationsDashboard {
            total_import_batches,
            open_issues,
            duplicate_issues,
            failed_jobs,
            compatibility_regressions,
            needs_review_release_instances,
            queries: recommended_queries(),
        })
    }
}

fn recommended_queries() -> Vec<DashboardQuery> {
    vec![
        DashboardQuery {
            name: "Import Outcomes".to_string(),
            description: "Import outcomes split by result.".to_string(),
            promql: "sum by (result) (imports_total)".to_string(),
        },
        DashboardQuery {
            name: "Open Issues".to_string(),
            description: "Current open issues partitioned by type.".to_string(),
            promql: "sum by (issue_type) (issue_count{state=\"open\"})".to_string(),
        },
        DashboardQuery {
            name: "Failed Jobs".to_string(),
            description: "Recent failed jobs by job type.".to_string(),
            promql: "sum by (job_type) (jobs_total{status=\"failed\"})".to_string(),
        },
        DashboardQuery {
            name: "Duplicate Detections".to_string(),
            description: "Duplicate-detection outcomes emitted by enrichment.".to_string(),
            promql: "sum by (result) (duplicate_detections_total)".to_string(),
        },
        DashboardQuery {
            name: "Compatibility Regressions".to_string(),
            description: "Compatibility verification failures by result.".to_string(),
            promql: "sum by (result) (compatibility_verification_failures_total)".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::application::repository::{
        ImportBatchRepository, IssueRepository, JobRepository, ReleaseInstanceRepository,
        RepositoryError,
    };
    use crate::domain::import_batch::ImportBatch;
    use crate::domain::issue::{Issue, IssueSubject};
    use crate::domain::job::{Job, JobSubject, JobTrigger, JobType};
    use crate::domain::release_instance::{
        BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
        TechnicalVariant,
    };
    use crate::support::ids::{ImportBatchId, IssueId, JobId, ReleaseInstanceId, SourceId};
    use crate::support::pagination::Page;

    #[test]
    fn dashboard_summarizes_core_health_counts() {
        let repository = InMemoryDashboardRepository::seeded();
        let dashboard = OperationsDashboardLoader::new(repository)
            .load()
            .expect("dashboard should load");

        assert_eq!(dashboard.total_import_batches, 2);
        assert_eq!(dashboard.open_issues, 3);
        assert_eq!(dashboard.duplicate_issues, 1);
        assert_eq!(dashboard.failed_jobs, 1);
        assert_eq!(dashboard.compatibility_regressions, 2);
        assert_eq!(dashboard.needs_review_release_instances, 1);
    }

    #[test]
    fn dashboard_exposes_recommended_prometheus_queries() {
        let queries = recommended_queries();

        assert!(queries.iter().any(|query| query.name == "Import Outcomes"));
        assert!(queries.iter().any(|query| {
            query.name == "Compatibility Regressions"
                && query
                    .promql
                    .contains("compatibility_verification_failures_total")
        }));
    }

    struct InMemoryDashboardRepository {
        import_batches: Arc<Vec<ImportBatch>>,
        issues: Arc<Vec<Issue>>,
        jobs: Arc<Vec<Job>>,
        release_instances: Arc<Vec<ReleaseInstance>>,
    }

    impl InMemoryDashboardRepository {
        fn seeded() -> Self {
            let source_id = SourceId::new();
            let batch_id = ImportBatchId::new();
            let release_instance_id = ReleaseInstanceId::new();

            Self {
                import_batches: Arc::new(vec![
                    ImportBatch {
                        id: batch_id.clone(),
                        source_id: source_id.clone(),
                        mode: crate::domain::import_batch::ImportMode::Copy,
                        status: crate::domain::import_batch::ImportBatchStatus::Submitted,
                        requested_by: crate::domain::import_batch::BatchRequester::Operator {
                            name: "operator".to_string(),
                        },
                        created_at_unix_seconds: 100,
                        received_paths: vec![],
                    },
                    ImportBatch {
                        id: ImportBatchId::new(),
                        source_id: source_id.clone(),
                        mode: crate::domain::import_batch::ImportMode::Copy,
                        status: crate::domain::import_batch::ImportBatchStatus::Submitted,
                        requested_by: crate::domain::import_batch::BatchRequester::Operator {
                            name: "operator".to_string(),
                        },
                        created_at_unix_seconds: 101,
                        received_paths: vec![],
                    },
                ]),
                issues: Arc::new(vec![
                    Issue::open(
                        IssueType::DuplicateReleaseInstance,
                        IssueSubject::ReleaseInstance(release_instance_id.clone()),
                        "dup",
                        None,
                        100,
                    ),
                    Issue::open(
                        IssueType::PlayerCompatibilityFailure,
                        IssueSubject::ReleaseInstance(release_instance_id.clone()),
                        "compat",
                        None,
                        100,
                    ),
                    Issue::open(
                        IssueType::PlayerVisibilityCollision,
                        IssueSubject::ReleaseInstance(release_instance_id.clone()),
                        "visibility",
                        None,
                        100,
                    ),
                ]),
                jobs: Arc::new(vec![
                    {
                        let mut job = Job::queued(
                            JobType::VerifyImport,
                            JobSubject::ImportBatch(batch_id),
                            JobTrigger::System,
                            100,
                        );
                        job.start("verify", 101).expect("job should start");
                        job.fail("verify", "failed", 102).expect("job should fail");
                        job
                    },
                    Job::queued(
                        JobType::DiscoverBatch,
                        JobSubject::ImportBatch(ImportBatchId::new()),
                        JobTrigger::System,
                        102,
                    ),
                ]),
                release_instances: Arc::new(vec![
                    ReleaseInstance {
                        id: release_instance_id,
                        import_batch_id: ImportBatchId::new(),
                        source_id,
                        release_id: None,
                        state: ReleaseInstanceState::NeedsReview,
                        technical_variant: TechnicalVariant {
                            format_family: FormatFamily::Flac,
                            bitrate_mode: BitrateMode::Lossless,
                            bitrate_kbps: None,
                            sample_rate_hz: Some(44_100),
                            bit_depth: Some(16),
                            track_count: 1,
                            total_duration_seconds: 100,
                        },
                        provenance: ProvenanceSnapshot {
                            ingest_origin: IngestOrigin::ManualAdd,
                            original_source_path: "/tmp".to_string(),
                            imported_at_unix_seconds: 100,
                            gazelle_reference: None,
                        },
                    },
                    ReleaseInstance {
                        id: ReleaseInstanceId::new(),
                        import_batch_id: ImportBatchId::new(),
                        source_id: SourceId::new(),
                        release_id: None,
                        state: ReleaseInstanceState::Verified,
                        technical_variant: TechnicalVariant {
                            format_family: FormatFamily::Flac,
                            bitrate_mode: BitrateMode::Lossless,
                            bitrate_kbps: None,
                            sample_rate_hz: Some(44_100),
                            bit_depth: Some(16),
                            track_count: 1,
                            total_duration_seconds: 100,
                        },
                        provenance: ProvenanceSnapshot {
                            ingest_origin: IngestOrigin::ManualAdd,
                            original_source_path: "/tmp".to_string(),
                            imported_at_unix_seconds: 100,
                            gazelle_reference: None,
                        },
                    },
                ]),
            }
        }
    }

    impl ImportBatchRepository for InMemoryDashboardRepository {
        fn get_import_batch(
            &self,
            id: &ImportBatchId,
        ) -> Result<Option<ImportBatch>, RepositoryError> {
            Ok(self
                .import_batches
                .iter()
                .find(|batch| batch.id == *id)
                .cloned())
        }

        fn list_import_batches(
            &self,
            query: &ImportBatchListQuery,
        ) -> Result<Page<ImportBatch>, RepositoryError> {
            Ok(Page {
                total: self.import_batches.len() as u64,
                items: self.import_batches.as_ref().clone(),
                request: query.page,
            })
        }
    }

    impl IssueRepository for InMemoryDashboardRepository {
        fn get_issue(&self, id: &IssueId) -> Result<Option<Issue>, RepositoryError> {
            Ok(self.issues.iter().find(|issue| issue.id == *id).cloned())
        }

        fn list_issues(&self, query: &IssueListQuery) -> Result<Page<Issue>, RepositoryError> {
            let items = self
                .issues
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
                .cloned()
                .collect::<Vec<_>>();
            Ok(Page {
                total: items.len() as u64,
                items,
                request: query.page,
            })
        }
    }

    impl JobRepository for InMemoryDashboardRepository {
        fn get_job(&self, id: &JobId) -> Result<Option<Job>, RepositoryError> {
            Ok(self.jobs.iter().find(|job| job.id == *id).cloned())
        }

        fn list_jobs(&self, query: &JobListQuery) -> Result<Page<Job>, RepositoryError> {
            let items = self
                .jobs
                .iter()
                .filter(|job| {
                    query
                        .status
                        .as_ref()
                        .is_none_or(|status| &job.status == status)
                })
                .filter(|job| {
                    query
                        .job_type
                        .as_ref()
                        .is_none_or(|job_type| &job.job_type == job_type)
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

    impl ReleaseInstanceRepository for InMemoryDashboardRepository {
        fn get_release_instance(
            &self,
            id: &ReleaseInstanceId,
        ) -> Result<Option<ReleaseInstance>, RepositoryError> {
            Ok(self
                .release_instances
                .iter()
                .find(|release_instance| release_instance.id == *id)
                .cloned())
        }

        fn list_release_instances(
            &self,
            query: &ReleaseInstanceListQuery,
        ) -> Result<Page<ReleaseInstance>, RepositoryError> {
            let items = self
                .release_instances
                .iter()
                .filter(|release_instance| {
                    query
                        .state
                        .as_ref()
                        .is_none_or(|state| &release_instance.state == state)
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
                total: 0,
                items: Vec::new(),
                request: PageRequest::default(),
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
}
