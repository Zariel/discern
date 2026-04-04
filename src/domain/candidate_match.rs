use crate::support::ids::{CandidateMatchId, ReleaseInstanceId};

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateMatch {
    pub id: CandidateMatchId,
    pub release_instance_id: ReleaseInstanceId,
    pub provider: CandidateProvider,
    pub subject: CandidateSubject,
    pub normalized_score: CandidateScore,
    pub evidence_matches: Vec<EvidenceNote>,
    pub mismatches: Vec<EvidenceNote>,
    pub unresolved_ambiguities: Vec<String>,
    pub provider_provenance: ProviderProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateProvider {
    MusicBrainz,
    Discogs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateSubject {
    Release { provider_id: String },
    ReleaseGroup { provider_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CandidateScore(f32);

impl CandidateScore {
    pub fn new(value: f32) -> Self {
        assert!(
            (0.0..=1.0).contains(&value),
            "candidate score must be in the range 0.0..=1.0"
        );
        Self(value)
    }

    pub fn value(self) -> f32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceNote {
    pub kind: EvidenceKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceKind {
    ArtistMatch,
    AlbumTitleMatch,
    TrackCountMatch,
    DurationAlignment,
    DiscCountMatch,
    DateProximity,
    LabelCatalogAlignment,
    FilenameSimilarity,
    GazelleConsistency,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderProvenance {
    pub provider_name: String,
    pub query: String,
    pub fetched_at_unix_seconds: i64,
}
