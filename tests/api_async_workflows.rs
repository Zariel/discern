use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};

use discern::api::ingest::{CreateImportBatchRequest, IngestApi};
use discern::api::issues::IssuesApi;
use discern::api::jobs::{
    JobStatusValue, JobsApi, ListJobsRequest, RetryJobRequest, RetryScopeValue,
};
use discern::api::review::{ListCandidateMatchesRequest, ReviewApi, SelectCandidateMatchRequest};
use discern::application::config::ValidatedRuntimeConfig;
use discern::application::jobs::JobService;
use discern::application::matching::{
    DiscogsMetadataProvider, DiscogsReleaseCandidate, DiscogsReleaseQuery, MusicBrainzArtistCredit,
    MusicBrainzLabelInfo, MusicBrainzMetadataProvider, MusicBrainzReleaseCandidate,
    MusicBrainzReleaseDetail, MusicBrainzReleaseGroupCandidate, MusicBrainzReleaseGroupRef,
};
use discern::application::repository::{
    ExportCommandRepository, ImportBatchCommandRepository, IssueCommandRepository, IssueRepository,
    ReleaseInstanceCommandRepository, ReleaseInstanceRepository, SourceCommandRepository,
};
use discern::config::AppConfig;
use discern::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
    EvidenceNote, ProviderProvenance,
};
use discern::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use discern::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
use discern::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use discern::domain::job::{JobSubject, JobTrigger, JobType};
use discern::domain::release_instance::{
    BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
    ReleaseInstanceState, TechnicalVariant,
};
use discern::domain::source::{Source, SourceKind, SourceLocator};
use discern::infrastructure::sqlite::{SqliteRepositories, SqliteRepositoryContext};
use discern::support::ids::{
    CandidateMatchId, ExportedMetadataSnapshotId, ImportBatchId, ReleaseInstanceId, SourceId,
};

#[test]
fn ingest_submission_and_job_queries_share_envelope_contract() {
    let root = temp_root("api-ingest-jobs");
    let config = test_config(&root);
    let repository = open_repositories(&config);
    let submitted_path = root.join("incoming/drop/Kid A/01-track.flac");
    write_file(&submitted_path, b"flac");

    let ingest_api = IngestApi::new(repository.clone(), config.clone());
    let jobs_api = JobsApi::new(repository.clone(), config);

    let submitted = ingest_api
        .create_import_batch(
            "req_ingest",
            CreateImportBatchRequest {
                client_name: "scanner".to_string(),
                submitted_paths: vec![submitted_path.display().to_string()],
                submitted_at_unix_seconds: 100,
            },
        )
        .expect("ingest submission should succeed");

    assert_eq!(submitted.meta.request_id, "req_ingest");
    assert!(submitted.meta.pagination.is_none());
    let submission = submitted.data.expect("submission data should exist");
    assert_eq!(
        submission.source.kind,
        discern::api::ingest::SourceKindValue::ApiClient
    );
    assert_eq!(
        submission.job.job_type,
        discern::api::jobs::JobTypeValue::DiscoverBatch
    );
    assert_eq!(submission.job.status, JobStatusValue::Queued);

    let listed = jobs_api
        .list_jobs(
            "req_jobs",
            ListJobsRequest {
                limit: 1,
                offset: 0,
                ..ListJobsRequest::default()
            },
        )
        .expect("job listing should succeed");

    assert_eq!(listed.meta.request_id, "req_jobs");
    let pagination = listed.meta.pagination.expect("pagination should exist");
    assert_eq!(pagination.limit, 1);
    assert_eq!(pagination.offset, 0);
    assert_eq!(pagination.total, 1);
    assert!(!pagination.has_more);
    let listed_jobs = listed.data.expect("job list should exist");
    assert_eq!(listed_jobs.len(), 1);
    assert_eq!(listed_jobs[0].id, submission.job.id);

    let fetched = jobs_api
        .get_job("req_job", &submission.job.id)
        .expect("job lookup should succeed");

    assert_eq!(fetched.meta.request_id, "req_job");
    let job = fetched.data.expect("job should exist");
    assert_eq!(job.id, submission.job.id);
    assert_eq!(job.subject.kind, "import_batch");
    assert_eq!(job.subject.reference, submission.batch.id);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn retry_job_requeues_work_and_resets_release_instance_state() {
    let root = temp_root("api-job-retry");
    let config = test_config(&root);
    let repository = open_repositories(&config);
    let seeded = seed_failed_match_job(&repository, &root);
    let jobs_api = JobsApi::new(repository.clone(), config);

    let retried = jobs_api
        .retry_job(
            "req_retry",
            &seeded.job.id.as_uuid().to_string(),
            RetryJobRequest {
                scope: RetryScopeValue::Rematch,
                queued_at_unix_seconds: 120,
            },
        )
        .expect("job retry should succeed");

    assert_eq!(retried.meta.request_id, "req_retry");
    let job = retried.data.expect("job should exist");
    assert_eq!(job.status, JobStatusValue::Queued);
    assert_eq!(job.retry_count, 1);

    let release_instance = repository
        .get_release_instance(&seeded.release_instance.id)
        .expect("release instance lookup should succeed")
        .expect("release instance should exist");
    assert_eq!(release_instance.state, ReleaseInstanceState::Analyzed);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "current_thread")]
async fn review_selection_and_issue_actions_persist_across_apis() {
    let root = temp_root("api-review-issues");
    let config = test_config(&root);
    let repository = open_repositories(&config);
    let seeded = seed_review_flow(&repository, &root);
    let review_api = ReviewApi::new(repository.clone(), TestMetadataProvider::default());
    let issues_api = IssuesApi::new(repository.clone());

    let listed = review_api
        .list_candidate_matches(
            "req_candidates",
            &seeded.release_instance.id.as_uuid().to_string(),
            ListCandidateMatchesRequest {
                limit: 1,
                offset: 0,
            },
        )
        .expect("candidate listing should succeed");

    assert_eq!(listed.meta.request_id, "req_candidates");
    let pagination = listed.meta.pagination.expect("pagination should exist");
    assert_eq!(pagination.total, 2);
    assert_eq!(pagination.next_offset, Some(1));

    let selected = review_api
        .select_candidate_match(
            "req_select",
            &seeded.release_instance.id.as_uuid().to_string(),
            &seeded.musicbrainz_candidate.id.as_uuid().to_string(),
            SelectCandidateMatchRequest {
                selected_by: "operator".to_string(),
                note: Some("canonical match confirmed".to_string()),
                selected_at_unix_seconds: 220,
            },
        )
        .await
        .expect("candidate selection should succeed");

    assert_eq!(selected.meta.request_id, "req_select");
    let resolution = selected.data.expect("resolution should exist");
    assert_eq!(
        resolution.selected_candidate_id,
        Some(seeded.musicbrainz_candidate.id.as_uuid().to_string())
    );
    assert_eq!(
        resolution.state,
        discern::api::inspection::ReleaseInstanceStateValue::Matched
    );

    let review_issue = repository
        .get_issue(&seeded.review_issue.id)
        .expect("issue lookup should succeed")
        .expect("review issue should exist");
    assert_eq!(review_issue.state, IssueState::Resolved);

    let detail = issues_api
        .get_issue(
            "req_issue_detail",
            &seeded.diagnostics_issue.id.as_uuid().to_string(),
        )
        .expect("issue detail should succeed");

    assert_eq!(detail.meta.request_id, "req_issue_detail");
    let issue_detail = detail.data.expect("issue detail should exist");
    let diagnostics = issue_detail
        .export_diagnostics
        .expect("export diagnostics should exist");
    assert_eq!(diagnostics.album_title, "Kid A");
    assert_eq!(
        diagnostics.path_components,
        vec!["Radiohead".to_string(), "Kid A".to_string()]
    );

    let resolved = issues_api
        .resolve_issue(
            "req_issue_resolve",
            &seeded.diagnostics_issue.id.as_uuid().to_string(),
            230,
        )
        .expect("issue resolve should succeed");

    assert_eq!(resolved.meta.request_id, "req_issue_resolve");
    let resolved_issue = resolved.data.expect("resolved issue should exist");
    assert_eq!(
        resolved_issue.state,
        discern::api::issues::IssueStateValue::Resolved
    );

    let _ = fs::remove_dir_all(root);
}

#[derive(Debug, Clone)]
struct SeededRetryState {
    release_instance: ReleaseInstance,
    job: discern::domain::job::Job,
}

#[derive(Debug, Clone)]
struct SeededReviewState {
    release_instance: ReleaseInstance,
    musicbrainz_candidate: CandidateMatch,
    review_issue: Issue,
    diagnostics_issue: Issue,
}

#[derive(Debug, Clone, Default)]
struct TestMetadataProvider;

impl MusicBrainzMetadataProvider for TestMetadataProvider {
    fn search_releases(
        &self,
        _query: &str,
        _limit: u8,
    ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseCandidate>, String>> + Send {
        async { Ok(Vec::new()) }
    }

    fn search_release_groups(
        &self,
        _query: &str,
        _limit: u8,
    ) -> impl Future<Output = Result<Vec<MusicBrainzReleaseGroupCandidate>, String>> + Send {
        async { Ok(Vec::new()) }
    }

    fn lookup_release(
        &self,
        release_id: &str,
    ) -> impl Future<Output = Result<MusicBrainzReleaseDetail, String>> + Send {
        let release_id = release_id.to_string();
        async move {
            Ok(MusicBrainzReleaseDetail {
                id: release_id,
                title: "Kid A".to_string(),
                country: Some("GB".to_string()),
                date: Some("2000-10-02".to_string()),
                artist_credit: vec![MusicBrainzArtistCredit {
                    artist_id: "mb-artist-1".to_string(),
                    artist_name: "Radiohead".to_string(),
                    artist_sort_name: "Radiohead".to_string(),
                }],
                release_group: Some(MusicBrainzReleaseGroupRef {
                    id: "mb-group-1".to_string(),
                    title: "Kid A".to_string(),
                    primary_type: Some("Album".to_string()),
                }),
                label_info: vec![MusicBrainzLabelInfo {
                    catalog_number: Some("XLLP782".to_string()),
                    label_name: Some("XL Recordings".to_string()),
                }],
            })
        }
    }
}

impl DiscogsMetadataProvider for TestMetadataProvider {
    fn search_releases(
        &self,
        _query: &DiscogsReleaseQuery,
        _limit: u8,
    ) -> impl Future<Output = Result<Vec<DiscogsReleaseCandidate>, String>> + Send {
        async { Ok(Vec::new()) }
    }
}

fn temp_root(prefix: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).expect("temp root should create");
    root
}

fn write_file(path: &Path, contents: &[u8]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parent directories should create");
    }
    fs::write(path, contents).expect("file should write");
}

fn test_config(root: &Path) -> ValidatedRuntimeConfig {
    let mut config = AppConfig::default();
    config.storage.sqlite_path = root.join("discern.db");
    config.storage.managed_library_root = root.join("library");
    config.storage.watch_directories[0].path = root.join("incoming");
    ValidatedRuntimeConfig::from_validated_app_config(&config)
}

fn open_repositories(config: &ValidatedRuntimeConfig) -> SqliteRepositories {
    fs::create_dir_all(&config.storage.managed_library_root)
        .expect("managed library root should create");
    for watcher in &config.storage.watch_directories {
        fs::create_dir_all(&watcher.path).expect("watch directory should create");
    }
    let context = SqliteRepositoryContext::open(config.storage.sqlite_path.clone())
        .expect("context should open");
    context.ensure_schema().expect("schema should initialize");
    SqliteRepositories::new(context)
}

fn seed_failed_match_job(repository: &SqliteRepositories, root: &Path) -> SeededRetryState {
    let source = manual_source(root);
    repository
        .create_source(&source)
        .expect("source should persist");

    let batch = ImportBatch {
        id: ImportBatchId::new(),
        source_id: source.id.clone(),
        mode: ImportMode::Copy,
        status: ImportBatchStatus::Failed,
        requested_by: BatchRequester::Operator {
            name: "operator".to_string(),
        },
        created_at_unix_seconds: 100,
        received_paths: vec![root.join("incoming/retry")],
    };
    repository
        .create_import_batch(&batch)
        .expect("batch should persist");

    let release_instance = ReleaseInstance {
        id: ReleaseInstanceId::new(),
        import_batch_id: batch.id.clone(),
        source_id: source.id,
        release_id: None,
        state: ReleaseInstanceState::NeedsReview,
        technical_variant: flac_variant(),
        provenance: ProvenanceSnapshot {
            ingest_origin: IngestOrigin::ManualAdd,
            original_source_path: root.join("incoming/retry").display().to_string(),
            imported_at_unix_seconds: 100,
            gazelle_reference: None,
        },
    };
    repository
        .create_release_instance(&release_instance)
        .expect("release instance should persist");

    let job_service = JobService::new(repository.clone());
    let job = job_service
        .enqueue_job(
            JobType::MatchReleaseInstance,
            JobSubject::ReleaseInstance(release_instance.id.clone()),
            JobTrigger::System,
            101,
        )
        .expect("job should queue");
    job_service
        .start_job(&job.id, "matching", 102)
        .expect("job should start");
    let job = job_service
        .fail_job(&job.id, "matching", "provider timeout", 103)
        .expect("job should fail");

    SeededRetryState {
        release_instance,
        job,
    }
}

fn seed_review_flow(repository: &SqliteRepositories, root: &Path) -> SeededReviewState {
    let source = manual_source(root);
    repository
        .create_source(&source)
        .expect("source should persist");

    let batch = ImportBatch {
        id: ImportBatchId::new(),
        source_id: source.id.clone(),
        mode: ImportMode::Copy,
        status: ImportBatchStatus::Grouped,
        requested_by: BatchRequester::Operator {
            name: "operator".to_string(),
        },
        created_at_unix_seconds: 200,
        received_paths: vec![root.join("incoming/review")],
    };
    repository
        .create_import_batch(&batch)
        .expect("batch should persist");

    let release_instance = ReleaseInstance {
        id: ReleaseInstanceId::new(),
        import_batch_id: batch.id,
        source_id: source.id,
        release_id: None,
        state: ReleaseInstanceState::NeedsReview,
        technical_variant: flac_variant(),
        provenance: ProvenanceSnapshot {
            ingest_origin: IngestOrigin::ManualAdd,
            original_source_path: root.join("incoming/review").display().to_string(),
            imported_at_unix_seconds: 200,
            gazelle_reference: None,
        },
    };
    repository
        .create_release_instance(&release_instance)
        .expect("release instance should persist");

    let musicbrainz_candidate = CandidateMatch {
        id: CandidateMatchId::new(),
        release_instance_id: release_instance.id.clone(),
        provider: CandidateProvider::MusicBrainz,
        subject: CandidateSubject::Release {
            provider_id: "mb-release-1".to_string(),
        },
        normalized_score: CandidateScore::new(0.97),
        evidence_matches: vec![EvidenceNote {
            kind: EvidenceKind::AlbumTitleMatch,
            detail: "title matched Kid A".to_string(),
        }],
        mismatches: Vec::new(),
        unresolved_ambiguities: Vec::new(),
        provider_provenance: ProviderProvenance {
            provider_name: "musicbrainz".to_string(),
            query: "kid a radiohead".to_string(),
            fetched_at_unix_seconds: 205,
        },
    };
    let discogs_candidate = CandidateMatch {
        id: CandidateMatchId::new(),
        release_instance_id: release_instance.id.clone(),
        provider: CandidateProvider::Discogs,
        subject: CandidateSubject::Release {
            provider_id: "discogs-release-1".to_string(),
        },
        normalized_score: CandidateScore::new(0.63),
        evidence_matches: vec![EvidenceNote {
            kind: EvidenceKind::LabelCatalogAlignment,
            detail: "label hint matched XL".to_string(),
        }],
        mismatches: vec![EvidenceNote {
            kind: EvidenceKind::DateProximity,
            detail: "year differed by one".to_string(),
        }],
        unresolved_ambiguities: vec!["edition differs".to_string()],
        provider_provenance: ProviderProvenance {
            provider_name: "discogs".to_string(),
            query: "kid a xl".to_string(),
            fetched_at_unix_seconds: 206,
        },
    };
    repository
        .replace_candidate_matches(
            &release_instance.id,
            &[musicbrainz_candidate.clone(), discogs_candidate],
        )
        .expect("candidate matches should persist");

    let review_issue = Issue::open(
        IssueType::AmbiguousReleaseMatch,
        IssueSubject::ReleaseInstance(release_instance.id.clone()),
        "Multiple matches require review",
        None,
        207,
    );
    repository
        .create_issue(&review_issue)
        .expect("review issue should persist");

    repository
        .create_exported_metadata_snapshot(&ExportedMetadataSnapshot {
            id: ExportedMetadataSnapshotId::new(),
            release_instance_id: release_instance.id.clone(),
            export_profile: "generic_player".to_string(),
            album_title: "Kid A".to_string(),
            album_artist: "Radiohead".to_string(),
            artist_credits: vec!["Radiohead".to_string()],
            edition_visibility: QualifierVisibility::TagsAndPath,
            technical_visibility: QualifierVisibility::PathOnly,
            path_components: vec!["Radiohead".to_string(), "Kid A".to_string()],
            primary_artwork_filename: Some("cover.jpg".to_string()),
            compatibility: CompatibilityReport {
                verified: true,
                warnings: Vec::new(),
            },
            rendered_at_unix_seconds: 208,
        })
        .expect("export snapshot should persist");

    let diagnostics_issue = Issue::open(
        IssueType::MissingArtwork,
        IssueSubject::ReleaseInstance(release_instance.id.clone()),
        "Artwork missing",
        Some("No primary artwork was selected".to_string()),
        209,
    );
    repository
        .create_issue(&diagnostics_issue)
        .expect("diagnostics issue should persist");

    SeededReviewState {
        release_instance,
        musicbrainz_candidate,
        review_issue,
        diagnostics_issue,
    }
}

fn manual_source(root: &Path) -> Source {
    Source {
        id: SourceId::new(),
        kind: SourceKind::ManualAdd,
        display_name: "manual:operator".to_string(),
        locator: SourceLocator::ManualEntry {
            submitted_path: root.join("incoming"),
        },
        external_reference: None,
    }
}

fn flac_variant() -> TechnicalVariant {
    TechnicalVariant {
        format_family: FormatFamily::Flac,
        bitrate_mode: BitrateMode::Lossless,
        bitrate_kbps: None,
        sample_rate_hz: Some(44_100),
        bit_depth: Some(16),
        track_count: 1,
        total_duration_seconds: 240,
    }
}
