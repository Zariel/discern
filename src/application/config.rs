use std::path::PathBuf;

use crate::config::{
    AppConfig, ArtworkConfig, ConfigValidationIssue, DiscogsConfig, DuplicatePolicy, ExportConfig,
    ExportProfileConfig, ImportConfig, MusicBrainzConfig, PathTemplateConfig, TaggingConfig,
    WatchDirectoryConfig, WorkerConfig,
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
    pub profiles: Vec<ExportProfileConfig>,
    pub path_templates: PathTemplateConfig,
    pub tagging: TaggingConfig,
    pub artwork: ArtworkConfig,
}

impl From<&ExportConfig> for ExportPolicy {
    fn from(config: &ExportConfig) -> Self {
        Self {
            default_profile: config.default_profile.clone(),
            profiles: config.profiles.clone(),
            path_templates: config.path_templates.clone(),
            tagging: config.tagging.clone(),
            artwork: config.artwork.clone(),
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
            VariantVisibilityPolicy::TagsAndPath
        );
        assert_eq!(validated.workers.provider_request_concurrency, 4);
        assert!(validated.diagnostics.is_empty());
    }
}
