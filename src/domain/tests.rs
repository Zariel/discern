use crate::domain::candidate_match::{
    CandidateMatch, CandidateProvider, CandidateScore, CandidateSubject, EvidenceKind,
    EvidenceNote, ProviderProvenance,
};
use crate::domain::config_snapshot::ConfigSnapshot;
use crate::domain::exported_metadata_snapshot::{
    CompatibilityReport, ExportedMetadataSnapshot, QualifierVisibility,
};
use crate::domain::file::{FileRecord, FileRole};
use crate::domain::import_batch::{BatchRequester, ImportBatch, ImportBatchStatus, ImportMode};
use crate::domain::issue::{Issue, IssueState, IssueSubject, IssueType};
use crate::domain::job::{Job, JobStatus, JobSubject, JobTrigger, JobType};
use crate::domain::manual_override::{ManualOverride, OverrideField, OverrideSubject};
use crate::domain::release::{Release, ReleaseEdition};
use crate::domain::release_artwork::{ArtworkSource, ReleaseArtwork};
use crate::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use crate::domain::release_instance::{
    BitrateMode, FormatFamily, IngestOrigin, ProvenanceSnapshot, ReleaseInstance,
    ReleaseInstanceState, TechnicalVariant,
};
use crate::domain::source::{Source, SourceKind, SourceLocator};
use crate::domain::track::{Track, TrackPosition};
use crate::domain::track_instance::{AudioProperties, TrackInstance};
use crate::support::ids::{
    ArtistId, CandidateMatchId, ConfigSnapshotId, ExportedMetadataSnapshotId, FileId,
    ImportBatchId, IssueId, JobId, ManualOverrideId, ReleaseArtworkId, ReleaseGroupId, ReleaseId,
    ReleaseInstanceId, SourceId, TrackId, TrackInstanceId,
};

#[test]
fn release_instance_is_distinct_from_release_and_release_group() {
    let artist_id = ArtistId::new();
    let release_group = ReleaseGroup {
        id: ReleaseGroupId::new(),
        primary_artist_id: artist_id.clone(),
        title: "Kid A".to_string(),
        kind: ReleaseGroupKind::Album,
        musicbrainz_release_group_id: None,
    };

    let release = Release {
        id: ReleaseId::new(),
        release_group_id: release_group.id.clone(),
        primary_artist_id: artist_id,
        title: "Kid A".to_string(),
        musicbrainz_release_id: None,
        discogs_release_id: None,
        edition: ReleaseEdition::default(),
    };

    let release_instance = ReleaseInstance {
        id: ReleaseInstanceId::new(),
        release_id: release.id.clone(),
        state: ReleaseInstanceState::Analyzed,
        technical_variant: TechnicalVariant {
            format_family: FormatFamily::Flac,
            bitrate_mode: BitrateMode::Lossless,
            bitrate_kbps: None,
            sample_rate_hz: Some(44_100),
            bit_depth: Some(16),
            track_count: 10,
            total_duration_seconds: 2_849,
        },
        provenance: ProvenanceSnapshot {
            ingest_origin: IngestOrigin::WatchDirectory,
            original_source_path: "/imports/radiohead/kid-a".to_string(),
            imported_at_unix_seconds: 1_712_288_000,
            gazelle_reference: None,
        },
    };

    assert_eq!(release.release_group_id, release_group.id);
    assert_eq!(release_instance.release_id, release.id);
    assert_ne!(release_instance.id.as_uuid(), release.id.as_uuid());
    assert_ne!(release.id.as_uuid(), release_group.id.as_uuid());
}

#[test]
fn track_instance_and_file_remain_attached_to_lower_level_entities() {
    let release_id = ReleaseId::new();
    let release_instance_id = ReleaseInstanceId::new();
    let track = Track {
        id: TrackId::new(),
        release_id: release_id.clone(),
        position: TrackPosition {
            disc_number: 1,
            track_number: 1,
        },
        title: "Everything in Its Right Place".to_string(),
        musicbrainz_track_id: None,
        duration_ms: Some(251_000),
    };

    let track_instance = TrackInstance {
        id: TrackInstanceId::new(),
        release_instance_id: release_instance_id.clone(),
        track_id: track.id.clone(),
        observed_position: track.position.clone(),
        observed_title: Some(track.title.clone()),
        audio_properties: AudioProperties {
            format_family: FormatFamily::Flac,
            duration_ms: track.duration_ms,
            bitrate_kbps: None,
            sample_rate_hz: Some(44_100),
            bit_depth: Some(16),
        },
    };

    let file = FileRecord {
        id: FileId::new(),
        track_instance_id: track_instance.id.clone(),
        role: FileRole::Source,
        format_family: FormatFamily::Flac,
        path: "/imports/radiohead/kid-a/01 - Everything in Its Right Place.flac".into(),
        checksum: Some("sha256:abc123".to_string()),
        size_bytes: 31_024_200,
    };

    assert_eq!(track.release_id, release_id);
    assert_eq!(track_instance.release_instance_id, release_instance_id);
    assert_eq!(track_instance.track_id, track.id);
    assert_eq!(file.track_instance_id, track_instance.id);
}

#[test]
fn supporting_entities_reference_domain_subjects_not_row_shapes() {
    let source = Source {
        id: SourceId::new(),
        kind: SourceKind::WatchDirectory,
        display_name: "watch import".to_string(),
        locator: SourceLocator::FilesystemPath("/watched/music".into()),
        external_reference: None,
    };

    let batch = ImportBatch {
        id: ImportBatchId::new(),
        source_id: source.id.clone(),
        mode: ImportMode::Copy,
        status: ImportBatchStatus::Created,
        requested_by: BatchRequester::System,
        created_at_unix_seconds: 1_712_288_100,
        received_paths: vec!["/watched/music/artist/album".into()],
    };

    let release_instance_id = ReleaseInstanceId::new();
    let issue = Issue {
        id: IssueId::new(),
        issue_type: IssueType::AmbiguousReleaseMatch,
        state: IssueState::Open,
        subject: IssueSubject::ReleaseInstance(release_instance_id.clone()),
        summary: "Multiple MusicBrainz candidates remain".to_string(),
        details: None,
        created_at_unix_seconds: 1_712_288_200,
        resolved_at_unix_seconds: None,
        suppressed_reason: None,
    };

    let job = Job {
        id: JobId::new(),
        job_type: JobType::MatchReleaseInstance,
        subject: JobSubject::ReleaseInstance(release_instance_id.clone()),
        status: JobStatus::Queued,
        progress_phase: "awaiting-dispatch".to_string(),
        retry_count: 0,
        triggered_by: JobTrigger::System,
        created_at_unix_seconds: 1_712_288_201,
        started_at_unix_seconds: None,
        finished_at_unix_seconds: None,
        error_payload: None,
    };

    assert_eq!(batch.source_id, source.id);
    assert_eq!(
        issue.subject,
        IssueSubject::ReleaseInstance(release_instance_id.clone())
    );
    assert_eq!(
        job.subject,
        JobSubject::ReleaseInstance(release_instance_id)
    );
}

#[test]
fn issues_track_resolution_and_suppression_lifecycle() {
    let subject = IssueSubject::Library;
    let mut resolved_issue = Issue::open(
        IssueType::MissingArtwork,
        subject.clone(),
        "Artwork missing",
        None,
        1_712_288_400,
    );
    resolved_issue
        .resolve(1_712_288_500)
        .expect("open issues should resolve");
    assert_eq!(resolved_issue.state, IssueState::Resolved);
    assert_eq!(resolved_issue.resolved_at_unix_seconds, Some(1_712_288_500));
    assert_eq!(resolved_issue.suppressed_reason, None);

    let mut suppressed_issue = Issue::open(
        IssueType::DuplicateReleaseInstance,
        subject,
        "Known duplicate",
        Some("Operator accepted overlap".to_string()),
        1_712_288_401,
    );
    suppressed_issue
        .suppress("intentional duplicate", 1_712_288_501)
        .expect("open issues should suppress");
    assert_eq!(suppressed_issue.state, IssueState::Suppressed);
    assert_eq!(
        suppressed_issue.suppressed_reason,
        Some("intentional duplicate".to_string())
    );
}

#[test]
fn exported_and_operator_state_attach_without_leaking_persistence_details() {
    let release_id = ReleaseId::new();
    let release_instance_id = ReleaseInstanceId::new();
    let track_id = TrackId::new();

    let snapshot = ExportedMetadataSnapshot {
        id: ExportedMetadataSnapshotId::new(),
        release_instance_id: release_instance_id.clone(),
        export_profile: "generic_player".to_string(),
        album_title: "Kid A [2011 CD]".to_string(),
        album_artist: "Radiohead".to_string(),
        artist_credits: vec!["Radiohead".to_string()],
        edition_visibility: QualifierVisibility::TagsAndPath,
        technical_visibility: QualifierVisibility::PathOnly,
        path_components: vec![
            "Radiohead".to_string(),
            "2000 - Kid A [2011 CD] [FLAC]".to_string(),
        ],
        primary_artwork_filename: Some("cover.jpg".to_string()),
        compatibility: CompatibilityReport {
            verified: true,
            warnings: Vec::new(),
        },
        rendered_at_unix_seconds: 1_712_288_300,
    };

    let override_record = ManualOverride {
        id: ManualOverrideId::new(),
        subject: OverrideSubject::Track(track_id.clone()),
        field: OverrideField::TrackTitle,
        value: "Everything in Its Right Place".to_string(),
        note: Some("Prefer CD booklet spelling".to_string()),
        created_by: "operator".to_string(),
        created_at_unix_seconds: 1_712_288_301,
    };

    let artwork = ReleaseArtwork {
        id: ReleaseArtworkId::new(),
        release_id: release_id.clone(),
        release_instance_id: Some(release_instance_id.clone()),
        source: ArtworkSource::OperatorSelected,
        is_primary: true,
        original_path: Some("/imports/kid-a/folder.jpg".into()),
        managed_filename: Some("cover.jpg".to_string()),
        mime_type: "image/jpeg".to_string(),
    };

    let config = ConfigSnapshot {
        id: ConfigSnapshotId::new(),
        release_instance_id: Some(release_instance_id.clone()),
        fingerprint: "sha256:config".to_string(),
        content: "export_profile = \"generic_player\"".to_string(),
        captured_at_unix_seconds: 1_712_288_302,
    };

    assert_eq!(snapshot.release_instance_id, release_instance_id);
    assert_eq!(override_record.subject, OverrideSubject::Track(track_id));
    assert_eq!(artwork.release_id, release_id);
    assert_eq!(config.release_instance_id, Some(release_instance_id));
}

#[test]
fn candidate_matches_attach_to_release_instance_with_scored_evidence() {
    let release_instance_id = ReleaseInstanceId::new();
    let candidate = CandidateMatch {
        id: CandidateMatchId::new(),
        release_instance_id: release_instance_id.clone(),
        provider: CandidateProvider::MusicBrainz,
        subject: CandidateSubject::Release {
            provider_id: "mb-release-123".to_string(),
        },
        normalized_score: CandidateScore::new(0.94),
        evidence_matches: vec![
            EvidenceNote {
                kind: EvidenceKind::ArtistMatch,
                detail: "artist matched exactly".to_string(),
            },
            EvidenceNote {
                kind: EvidenceKind::TrackCountMatch,
                detail: "track count matched 10 tracks".to_string(),
            },
        ],
        mismatches: vec![EvidenceNote {
            kind: EvidenceKind::DateProximity,
            detail: "source tags suggest a later reissue".to_string(),
        }],
        unresolved_ambiguities: vec!["2011 CD and 2012 repress remain close".to_string()],
        provider_provenance: ProviderProvenance {
            provider_name: "musicbrainz".to_string(),
            query: "artist=Radiohead album=Kid A".to_string(),
            fetched_at_unix_seconds: 1_712_288_400,
        },
    };

    assert_eq!(candidate.release_instance_id, release_instance_id);
    assert_eq!(candidate.normalized_score.value(), 0.94);
    assert!(matches!(
        candidate.subject,
        CandidateSubject::Release { .. }
    ));
    assert_eq!(candidate.evidence_matches.len(), 2);
    assert_eq!(candidate.mismatches.len(), 1);
}
