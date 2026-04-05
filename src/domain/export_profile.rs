#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportProfile {
    pub name: String,
    pub exported_fields: Vec<PlayerMetadataField>,
    pub edition_visibility: EditionVisibilityPolicy,
    pub technical_visibility: QualifierVisibilityPolicy,
    pub provenance_visibility: QualifierVisibilityPolicy,
    pub compilation_handling: CompilationHandling,
    pub write_internal_ids: bool,
    pub artwork: ArtworkPolicy,
}

impl ExportProfile {
    pub fn generic_player() -> Self {
        Self {
            name: "generic_player".to_string(),
            exported_fields: vec![
                PlayerMetadataField::Album,
                PlayerMetadataField::AlbumArtist,
                PlayerMetadataField::Artist,
                PlayerMetadataField::Title,
                PlayerMetadataField::TrackNumber,
                PlayerMetadataField::TotalTracks,
                PlayerMetadataField::DiscNumber,
                PlayerMetadataField::TotalDiscs,
                PlayerMetadataField::Date,
                PlayerMetadataField::Genre,
                PlayerMetadataField::MusicBrainzIdentifiers,
            ],
            edition_visibility: EditionVisibilityPolicy::AlbumTitleWhenNeeded,
            technical_visibility: QualifierVisibilityPolicy::PathOnly,
            provenance_visibility: QualifierVisibilityPolicy::Hidden,
            compilation_handling: CompilationHandling::StandardCompilationTags,
            write_internal_ids: true,
            artwork: ArtworkPolicy::SidecarFile {
                file_name: "cover.jpg".to_string(),
                embed_in_tags: false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlayerMetadataField {
    Album,
    AlbumArtist,
    Artist,
    Title,
    TrackNumber,
    TotalTracks,
    DiscNumber,
    TotalDiscs,
    Date,
    Genre,
    MusicBrainzIdentifiers,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditionVisibilityPolicy {
    Hidden,
    AlbumTitleWhenNeeded,
    AlbumTitleAlways,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualifierVisibilityPolicy {
    Hidden,
    PathOnly,
    TagsAndPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompilationHandling {
    StandardCompilationTags,
    AlbumArtistOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtworkPolicy {
    SidecarFile {
        file_name: String,
        embed_in_tags: bool,
    },
}
