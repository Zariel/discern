use std::path::PathBuf;

use crate::config::{
    AppConfig, ConfigValidationIssue, DiscogsConfig, DuplicatePolicy, ExportConfig, ImportConfig,
    MusicBrainzConfig, WatchDirectoryConfig, WorkerConfig,
};
use crate::domain::export_profile::{
    ArtworkPolicy, CompilationHandling, EditionVisibilityPolicy, ExportProfile,
    QualifierVisibilityPolicy,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigDiagnostics {
    pub issues: Vec<ConfigDiagnostic>,
}

impl ConfigDiagnostics {
    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDiagnostic {
    pub field: String,
    pub message: String,
}

impl From<ConfigValidationIssue> for ConfigDiagnostic {
    fn from(issue: ConfigValidationIssue) -> Self {
        Self {
            field: issue.field,
            message: issue.message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedRuntimeConfig {
    pub storage: StoragePolicy,
    pub import: ImportPolicy,
    pub export: ExportPolicy,
    pub providers: ProviderPolicy,
    pub workers: WorkerPolicy,
    pub diagnostics: ConfigDiagnostics,
}

impl ValidatedRuntimeConfig {
    pub fn from_validated_app_config(config: &AppConfig) -> Self {
        Self {
            storage: StoragePolicy::from(&config.storage),
            import: ImportPolicy::from(&config.import),
            export: ExportPolicy::from(&config.export),
            providers: ProviderPolicy::from(&config.providers),
            workers: WorkerPolicy::from(&config.workers),
            diagnostics: ConfigDiagnostics::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePolicy {
    pub sqlite_path: PathBuf,
    pub managed_library_root: PathBuf,
    pub watch_directories: Vec<WatchDirectoryPolicy>,
}

impl From<&crate::config::StorageConfig> for StoragePolicy {
    fn from(config: &crate::config::StorageConfig) -> Self {
        Self {
            sqlite_path: config.sqlite_path.clone(),
            managed_library_root: config.managed_library_root.clone(),
            watch_directories: config
                .watch_directories
                .iter()
                .map(WatchDirectoryPolicy::from)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchDirectoryPolicy {
    pub name: String,
    pub path: PathBuf,
    pub scan_mode: crate::config::WatchScanMode,
    pub import_mode_override: Option<crate::domain::import_batch::ImportMode>,
}

impl From<&WatchDirectoryConfig> for WatchDirectoryPolicy {
    fn from(config: &WatchDirectoryConfig) -> Self {
        Self {
            name: config.name.clone(),
            path: config.path.clone(),
            scan_mode: config.scan_mode.clone(),
            import_mode_override: config.import_mode_override.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportPolicy {
    pub default_mode: crate::domain::import_batch::ImportMode,
    pub duplicate_policy: DuplicatePolicy,
    pub supported_formats: Vec<crate::domain::release_instance::FormatFamily>,
}

impl From<&ImportConfig> for ImportPolicy {
    fn from(config: &ImportConfig) -> Self {
        Self {
            default_mode: config.default_mode.clone(),
            duplicate_policy: config.duplicate_policy.clone(),
            supported_formats: config.supported_formats.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPolicy {
    pub default_profile: String,
    pub profiles: Vec<ExportProfile>,
    pub path_templates: PathPolicy,
    pub tagging: TaggingPolicy,
}

impl From<&ExportConfig> for ExportPolicy {
    fn from(config: &ExportConfig) -> Self {
        Self {
            default_profile: config.default_profile.clone(),
            profiles: config
                .profiles
                .iter()
                .map(|profile| ExportProfile::from_config(profile, &config.artwork))
                .collect(),
            path_templates: PathPolicy::from(&config.path_templates),
            tagging: TaggingPolicy::from((&config.tagging, &config.artwork)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathPolicy {
    pub release_template: String,
    pub release_instance_template: String,
    pub character_replacement: String,
    pub max_path_length: usize,
}

impl From<&crate::config::PathTemplateConfig> for PathPolicy {
    fn from(config: &crate::config::PathTemplateConfig) -> Self {
        Self {
            release_template: config.release_template.clone(),
            release_instance_template: config.release_instance_template.clone(),
            character_replacement: config.character_replacement.clone(),
            max_path_length: config.max_path_length,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggingPolicy {
    pub mp3_id3v2_version: crate::config::Id3v2Version,
    pub unknown_tag_policy: crate::config::UnknownTagPolicy,
    pub selected_tag_keys: Vec<String>,
    pub artwork: ArtworkPolicy,
}

impl From<(&crate::config::TaggingConfig, &crate::config::ArtworkConfig)> for TaggingPolicy {
    fn from(
        (tagging, artwork): (&crate::config::TaggingConfig, &crate::config::ArtworkConfig),
    ) -> Self {
        Self {
            mp3_id3v2_version: tagging.mp3_id3v2_version.clone(),
            unknown_tag_policy: tagging.unknown_tag_policy.clone(),
            selected_tag_keys: tagging.selected_tag_keys.clone(),
            artwork: ArtworkPolicy::from(artwork),
        }
    }
}

impl ExportProfile {
    fn from_config(
        config: &crate::config::ExportProfileConfig,
        artwork: &crate::config::ArtworkConfig,
    ) -> Self {
        Self {
            name: config.name.clone(),
            exported_fields: ExportProfile::generic_player().exported_fields,
            edition_visibility: match config.edition_visibility {
                crate::config::EditionVisibilityPolicy::Hidden => EditionVisibilityPolicy::Hidden,
                crate::config::EditionVisibilityPolicy::AlbumTitleWhenNeeded => {
                    EditionVisibilityPolicy::AlbumTitleWhenNeeded
                }
                crate::config::EditionVisibilityPolicy::AlbumTitleAlways => {
                    EditionVisibilityPolicy::AlbumTitleAlways
                }
            },
            technical_visibility: map_variant_visibility(&config.technical_visibility),
            provenance_visibility: map_variant_visibility(&config.provenance_visibility),
            compilation_handling: match config.compilation_mode {
                crate::config::CompilationMode::StandardCompilationTags => {
                    CompilationHandling::StandardCompilationTags
                }
                crate::config::CompilationMode::AlbumArtistOnly => {
                    CompilationHandling::AlbumArtistOnly
                }
            },
            write_internal_ids: config.write_musicbrainz_ids,
            artwork: ArtworkPolicy::from(artwork),
        }
    }
}

impl From<&crate::config::ArtworkConfig> for ArtworkPolicy {
    fn from(config: &crate::config::ArtworkConfig) -> Self {
        Self::SidecarFile {
            file_name: config.sidecar_file_name.clone(),
            embed_in_tags: config.embed_in_tags,
        }
    }
}

fn map_variant_visibility(
    policy: &crate::config::VariantVisibilityPolicy,
) -> QualifierVisibilityPolicy {
    match policy {
        crate::config::VariantVisibilityPolicy::Hidden => QualifierVisibilityPolicy::Hidden,
        crate::config::VariantVisibilityPolicy::PathOnly => QualifierVisibilityPolicy::PathOnly,
        crate::config::VariantVisibilityPolicy::TagsAndPath => {
            QualifierVisibilityPolicy::TagsAndPath
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderPolicy {
    pub musicbrainz: MusicBrainzConfig,
    pub discogs: DiscogsConfig,
}

impl From<&crate::config::ProviderConfig> for ProviderPolicy {
    fn from(config: &crate::config::ProviderConfig) -> Self {
        Self {
            musicbrainz: config.musicbrainz.clone(),
            discogs: config.discogs.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPolicy {
    pub max_concurrent_jobs: usize,
    pub file_io_concurrency: usize,
    pub provider_request_concurrency: usize,
    pub db_write_concurrency: usize,
}

impl From<&WorkerConfig> for WorkerPolicy {
    fn from(config: &WorkerConfig) -> Self {
        Self {
            max_concurrent_jobs: config.max_concurrent_jobs,
            file_io_concurrency: config.file_io_concurrency,
            provider_request_concurrency: config.provider_request_concurrency,
            db_write_concurrency: config.db_write_concurrency,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::{AppConfig, DuplicatePolicy, VariantVisibilityPolicy};
    use crate::domain::export_profile::{
        ArtworkPolicy, EditionVisibilityPolicy, QualifierVisibilityPolicy,
    };

    use super::ValidatedRuntimeConfig;

    #[test]
    fn validated_runtime_config_normalizes_service_policies() {
        let mut config = AppConfig::default();
        config.storage.managed_library_root = PathBuf::from("/music/library");
        config.import.duplicate_policy = DuplicatePolicy::Flag;
        config.export.profiles[0].provenance_visibility = VariantVisibilityPolicy::TagsAndPath;
        config.workers.provider_request_concurrency = 4;

        let validated = ValidatedRuntimeConfig::from_validated_app_config(&config);

        assert_eq!(
            validated.storage.managed_library_root,
            PathBuf::from("/music/library")
        );
        assert_eq!(validated.import.duplicate_policy, DuplicatePolicy::Flag);
        assert_eq!(
            validated.export.profiles[0].provenance_visibility,
            QualifierVisibilityPolicy::TagsAndPath
        );
        assert_eq!(validated.workers.provider_request_concurrency, 4);
        assert!(validated.diagnostics.is_empty());
    }

    #[test]
    fn validated_runtime_config_normalizes_export_profile_models() {
        let mut config = AppConfig::default();
        config.export.profiles[0].edition_visibility =
            crate::config::EditionVisibilityPolicy::AlbumTitleAlways;
        config.export.profiles[0].technical_visibility = VariantVisibilityPolicy::TagsAndPath;
        config.export.artwork.sidecar_file_name = "front.jpg".to_string();
        config.export.artwork.embed_in_tags = true;

        let validated = ValidatedRuntimeConfig::from_validated_app_config(&config);

        assert_eq!(
            validated.export.profiles[0].edition_visibility,
            EditionVisibilityPolicy::AlbumTitleAlways
        );
        assert_eq!(
            validated.export.profiles[0].technical_visibility,
            QualifierVisibilityPolicy::TagsAndPath
        );
        assert_eq!(
            validated.export.profiles[0].artwork,
            ArtworkPolicy::SidecarFile {
                file_name: "front.jpg".to_string(),
                embed_in_tags: true,
            }
        );
    }
}
