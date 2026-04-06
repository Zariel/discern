use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};

use discern::api::ingest::CreateImportBatchFromPathRequest;
use discern::api::review::SelectCandidateMatchRequest;
use discern::application::config::ValidatedRuntimeConfig;
use discern::application::jobs::JobService;
use discern::application::matching::{
    DiscogsMetadataProvider, DiscogsReleaseCandidate, DiscogsReleaseQuery, MusicBrainzArtistCredit,
    MusicBrainzLabelInfo, MusicBrainzMetadataProvider, MusicBrainzReleaseCandidate,
    MusicBrainzReleaseDetail, MusicBrainzReleaseGroupCandidate, MusicBrainzReleaseGroupRef,
};
use discern::application::repository::{
    ExportCommandRepository, ImportBatchCommandRepository, IssueCommandRepository, IssueRepository,
    ReleaseCommandRepository, ReleaseInstanceCommandRepository, SourceCommandRepository,
};
use discern::config::AppConfig;
use discern::domain::artist::Artist;
use discern::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
    EvidenceNote, ProviderProvenance,
};
use discern::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use discern::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
use discern::domain::issue::{Issue, IssueSubject, IssueType};
use discern::domain::job::{JobSubject, JobTrigger, JobType};
use discern::domain::release::{Release, ReleaseEdition};
use discern::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use discern::domain::release_instance::{
    BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
    ReleaseInstanceState, TechnicalVariant,
};
use discern::domain::source::{Source, SourceKind, SourceLocator};
use discern::infrastructure::sqlite::{SqliteRepositories, SqliteRepositoryContext};
use discern::support::ids::{ImportBatchId, ReleaseInstanceId};
use discern::web::{
    CandidateReviewFilters, CandidateReviewScreenLoader, ExportPreviewScreenLoader,
    JobsScreenFilters, JobsScreenLoader, ManualImportBatchesFilters, ManualImportScreenLoader,
};

#[test]
fn manual_import_flow_appears_in_batches_and_jobs() {
    let root = temp_root("web-manual-import");
    let config = test_config(&root);
    let repository = open_repositories(&config);
    let submitted_path = root.join("incoming/manual/Kid A");
    fs::create_dir_all(&submitted_path).expect("manual path should exist");
    write_file(&submitted_path.join("01-track.flac"), b"flac");

    let import_loader = ManualImportScreenLoader::new(repository.clone(), config.clone());
    let jobs_loader = JobsScreenLoader::new(repository.clone(), config.clone());

    let submission = import_loader
        .submit_manual_path(
            "req_manual",
            CreateImportBatchFromPathRequest {
                operator_name: "operator".to_string(),
                submitted_path: submitted_path.display().to_string(),
                submitted_at_unix_seconds: 100,
            },
        )
        .expect("manual import should succeed");

    let import_screen = import_loader
        .load(
            "req_batches",
            ManualImportBatchesFilters {
                limit: 10,
                offset: 0,
            },
        )
        .expect("manual import screen should load");
    let jobs_screen = jobs_loader
        .load(
            "req_jobs",
            JobsScreenFilters {
                limit: 10,
                offset: 0,
                ..JobsScreenFilters::default()
            },
        )
        .expect("jobs screen should load");

    assert_eq!(submission.batch.requested_by.kind, "operator");
    assert_eq!(import_screen.total_batches, 1);
    assert_eq!(import_screen.recent_batches[0].id, submission.batch.id);
    assert_eq!(jobs_screen.summary.total, 1);
    assert_eq!(jobs_screen.items[0].subject.reference, submission.batch.id);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test(flavor = "current_thread")]
async fn review_and_preview_screens_load_from_persisted_sqlite_state() {
    let root = temp_root("web-review-preview");
    let config = test_config(&root);
    let repository = open_repositories(&config);
    let seeded = seed_review_preview_state(&repository, &root);

    let review_loader =
        CandidateReviewScreenLoader::new(repository.clone(), TestMetadataProvider::default());
    let preview_loader = ExportPreviewScreenLoader::new(repository.clone());

    let review_screen = review_loader
        .load(
            "req_review",
            &seeded.release_instance.id.as_uuid().to_string(),
            CandidateReviewFilters {
                selected_candidate_id: Some(seeded.candidate.id.as_uuid().to_string()),
                limit: 10,
                offset: 0,
            },
        )
        .expect("review screen should load");
    let resolution = review_loader
        .select_candidate(
            "req_select",
            &seeded.release_instance.id.as_uuid().to_string(),
            &seeded.candidate.id.as_uuid().to_string(),
            SelectCandidateMatchRequest {
                selected_by: "operator".to_string(),
                note: Some("confirmed".to_string()),
                selected_at_unix_seconds: 230,
            },
        )
        .await
        .expect("selection should succeed");
    let preview_screen = preview_loader
        .load(
            "req_preview",
            &seeded.release_instance.id.as_uuid().to_string(),
        )
        .expect("preview screen should load");

    assert_eq!(review_screen.total_candidates, 1);
    assert_eq!(
        review_screen
            .selected_candidate
            .expect("selected candidate should exist")
            .id,
        seeded.candidate.id.as_uuid().to_string()
    );
    assert_eq!(
        resolution.selected_candidate_id,
        Some(seeded.candidate.id.as_uuid().to_string())
    );
    assert_eq!(preview_screen.preview.album_title, "Kid A");
    assert_eq!(preview_screen.managed_path, "Radiohead/Kid A");

    let review_issue = repository
        .get_issue(&seeded.review_issue.id)
        .expect("issue lookup should succeed")
        .expect("issue should exist");
    assert_eq!(
        review_issue.state,
        discern::domain::issue::IssueState::Resolved
    );

    let _ = fs::remove_dir_all(root);
}

#[derive(Debug, Clone)]
struct SeededReviewPreviewState {
    release_instance: ReleaseInstance,
    candidate: CandidateMatch,
    review_issue: Issue,
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

fn seed_review_preview_state(
    repository: &SqliteRepositories,
    root: &Path,
) -> SeededReviewPreviewState {
    let source = Source {
        id: discern::support::ids::SourceId::new(),
        kind: SourceKind::ManualAdd,
        display_name: "Manual Add".to_string(),
        locator: SourceLocator::ManualEntry {
            submitted_path: root.join("incoming/review"),
        },
        external_reference: None,
    };
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

    let group = ReleaseGroup {
        id: discern::support::ids::ReleaseGroupId::new(),
        primary_artist_id: discern::support::ids::ArtistId::new(),
        title: "Kid A".to_string(),
        kind: ReleaseGroupKind::Album,
        musicbrainz_release_group_id: None,
    };
    repository
        .create_artist(&Artist {
            id: group.primary_artist_id.clone(),
            name: "Radiohead".to_string(),
            sort_name: Some("Radiohead".to_string()),
            musicbrainz_artist_id: None,
        })
        .expect("artist should persist");
    repository
        .create_release_group(&group)
        .expect("group should persist");

    let release = Release {
        id: discern::support::ids::ReleaseId::new(),
        release_group_id: group.id.clone(),
        primary_artist_id: group.primary_artist_id.clone(),
        title: "Kid A".to_string(),
        musicbrainz_release_id: None,
        discogs_release_id: None,
        edition: ReleaseEdition {
            edition_title: None,
            disambiguation: None,
            country: Some("GB".to_string()),
            label: Some("XL".to_string()),
            catalog_number: Some("XLLP782".to_string()),
            release_date: None,
        },
    };
    repository
        .create_release(&release)
        .expect("release should persist");

    let release_instance = ReleaseInstance {
        id: ReleaseInstanceId::new(),
        import_batch_id: batch.id.clone(),
        source_id: source.id.clone(),
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

    let candidate = CandidateMatch {
        id: discern::support::ids::CandidateMatchId::new(),
        release_instance_id: release_instance.id.clone(),
        provider: CandidateProvider::MusicBrainz,
        subject: CandidateSubject::Release {
            provider_id: "mb-release-1".to_string(),
        },
        normalized_score: CandidateScore::new(0.95),
        evidence_matches: vec![EvidenceNote {
            kind: EvidenceKind::AlbumTitleMatch,
            detail: "title aligned with local tags".to_string(),
        }],
        mismatches: Vec::new(),
        unresolved_ambiguities: Vec::new(),
        provider_provenance: ProviderProvenance {
            provider_name: "musicbrainz".to_string(),
            query: "kid a radiohead".to_string(),
            fetched_at_unix_seconds: 210,
        },
    };
    repository
        .replace_candidate_matches(&release_instance.id, &[candidate.clone()])
        .expect("candidates should persist");

    let review_issue = Issue::open(
        IssueType::AmbiguousReleaseMatch,
        IssueSubject::ReleaseInstance(release_instance.id.clone()),
        "Ambiguous release match",
        None,
        205,
    );
    repository
        .create_issue(&review_issue)
        .expect("issue should persist");

    repository
        .create_exported_metadata_snapshot(&ExportedMetadataSnapshot {
            id: discern::support::ids::ExportedMetadataSnapshotId::new(),
            release_instance_id: release_instance.id.clone(),
            export_profile: "generic_player".to_string(),
            album_title: "Kid A".to_string(),
            album_artist: "Radiohead".to_string(),
            artist_credits: vec!["Radiohead".to_string()],
            edition_visibility: QualifierVisibility::Hidden,
            technical_visibility: QualifierVisibility::Hidden,
            path_components: vec!["Radiohead".to_string(), "Kid A".to_string()],
            primary_artwork_filename: Some("cover.jpg".to_string()),
            compatibility: CompatibilityReport {
                verified: true,
                warnings: Vec::new(),
            },
            rendered_at_unix_seconds: 220,
        })
        .expect("export preview should persist");

    let job_service = JobService::new(repository.clone());
    let _ = job_service
        .enqueue_job(
            JobType::MatchReleaseInstance,
            JobSubject::ReleaseInstance(release_instance.id.clone()),
            JobTrigger::System,
            201,
        )
        .expect("job should queue");

    SeededReviewPreviewState {
        release_instance,
        candidate,
        review_issue,
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
        total_duration_seconds: 250,
    }
}
