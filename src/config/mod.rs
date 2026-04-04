use std::path::PathBuf;

use crate::domain::import_batch::ImportMode;
use crate::domain::release_instance::FormatFamily;

pub const GENERIC_PLAYER_PROFILE: &str = "generic_player";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppConfig {
    pub storage: StorageConfig,
    pub import: ImportConfig,
    pub export: ExportConfig,
    pub providers: ProviderConfig,
    pub workers: WorkerConfig,
    pub server: ServerConfig,
    pub api: ApiConfig,
    pub web: WebConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_base_path("api.base_path", &self.api.base_path)?;
        validate_base_path("web.mount_path", &self.web.mount_path)?;

        if self.web.asset_dir.as_os_str().is_empty() {
            return Err(ConfigError::new(
                "web.asset_dir",
                "web asset directory must not be empty",
            ));
        }

        if self.storage.managed_library_root.as_os_str().is_empty() {
            return Err(ConfigError::new(
                "storage.managed_library_root",
                "managed library root must not be empty",
            ));
        }

        if self.import.supported_formats.is_empty() {
            return Err(ConfigError::new(
                "import.supported_formats",
                "supported formats must not be empty",
            ));
        }

        if self.workers.max_concurrent_jobs == 0 {
            return Err(ConfigError::new(
                "workers.max_concurrent_jobs",
                "worker concurrency must be greater than zero",
            ));
        }

        if self.export.profiles.is_empty() {
            return Err(ConfigError::new(
                "export.profiles",
                "at least one export profile is required",
            ));
        }

        if !self
            .export
            .profiles
            .iter()
            .any(|profile| profile.name == self.export.default_profile)
        {
            return Err(ConfigError::new(
                "export.default_profile",
                "default export profile must exist in export.profiles",
            ));
        }

        if self.export.artwork.sidecar_file_name.trim().is_empty() {
            return Err(ConfigError::new(
                "export.artwork.sidecar_file_name",
                "sidecar artwork filename must not be empty",
            ));
        }

        if self.export.tagging.unknown_tag_policy == UnknownTagPolicy::PreserveSelected
            && self.export.tagging.selected_tag_keys.is_empty()
        {
            return Err(ConfigError::new(
                "export.tagging.selected_tag_keys",
                "preserve_selected requires at least one selected tag key",
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub sqlite_path: PathBuf,
    pub managed_library_root: PathBuf,
    pub watch_directories: Vec<WatchDirectoryConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_path: PathBuf::from("discern.db"),
            managed_library_root: PathBuf::from("library"),
            watch_directories: vec![WatchDirectoryConfig::default()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchDirectoryConfig {
    pub name: String,
    pub path: PathBuf,
    pub scan_mode: WatchScanMode,
    pub import_mode_override: Option<ImportMode>,
}

impl Default for WatchDirectoryConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            path: PathBuf::from("incoming"),
            scan_mode: WatchScanMode::EventDriven,
            import_mode_override: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchScanMode {
    EventDriven,
    PollingOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportConfig {
    pub default_mode: ImportMode,
    pub duplicate_policy: DuplicatePolicy,
    pub supported_formats: Vec<FormatFamily>,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            default_mode: ImportMode::Copy,
            duplicate_policy: DuplicatePolicy::AllowIfDistinguishable,
            supported_formats: vec![FormatFamily::Flac, FormatFamily::Mp3],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuplicatePolicy {
    AllowIfDistinguishable,
    Flag,
    Quarantine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportConfig {
    pub default_profile: String,
    pub profiles: Vec<ExportProfileConfig>,
    pub path_templates: PathTemplateConfig,
    pub tagging: TaggingConfig,
    pub artwork: ArtworkConfig,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            default_profile: GENERIC_PLAYER_PROFILE.to_string(),
            profiles: vec![ExportProfileConfig::default()],
            path_templates: PathTemplateConfig::default(),
            tagging: TaggingConfig::default(),
            artwork: ArtworkConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportProfileConfig {
    pub name: String,
    pub edition_visibility: EditionVisibilityPolicy,
    pub technical_visibility: VariantVisibilityPolicy,
    pub provenance_visibility: VariantVisibilityPolicy,
    pub compilation_mode: CompilationMode,
    pub write_musicbrainz_ids: bool,
}

impl Default for ExportProfileConfig {
    fn default() -> Self {
        Self {
            name: GENERIC_PLAYER_PROFILE.to_string(),
            edition_visibility: EditionVisibilityPolicy::AlbumTitleWhenNeeded,
            technical_visibility: VariantVisibilityPolicy::PathOnly,
            provenance_visibility: VariantVisibilityPolicy::Hidden,
            compilation_mode: CompilationMode::StandardCompilationTags,
            write_musicbrainz_ids: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditionVisibilityPolicy {
    Hidden,
    AlbumTitleWhenNeeded,
    AlbumTitleAlways,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariantVisibilityPolicy {
    Hidden,
    PathOnly,
    TagsAndPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompilationMode {
    StandardCompilationTags,
    AlbumArtistOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplateConfig {
    pub release_template: String,
    pub release_instance_template: String,
    pub character_replacement: String,
    pub max_path_length: usize,
}

impl Default for PathTemplateConfig {
    fn default() -> Self {
        Self {
            release_template: "{album_artist}/{release_title}".to_string(),
            release_instance_template: "{release_year} - {edition_label}/{format_family}"
                .to_string(),
            character_replacement: "_".to_string(),
            max_path_length: 240,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggingConfig {
    pub mp3_id3v2_version: Id3v2Version,
    pub unknown_tag_policy: UnknownTagPolicy,
    pub selected_tag_keys: Vec<String>,
}

impl Default for TaggingConfig {
    fn default() -> Self {
        Self {
            mp3_id3v2_version: Id3v2Version::V24,
            unknown_tag_policy: UnknownTagPolicy::PreserveSelected,
            selected_tag_keys: vec!["musicbrainz_albumid".to_string()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Id3v2Version {
    V23,
    V24,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnknownTagPolicy {
    DropUnknown,
    PreserveUnknown,
    PreserveSelected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtworkConfig {
    pub sidecar_file_name: String,
    pub embed_in_tags: bool,
}

impl Default for ArtworkConfig {
    fn default() -> Self {
        Self {
            sidecar_file_name: "cover.jpg".to_string(),
            embed_in_tags: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderConfig {
    pub musicbrainz: MusicBrainzConfig,
    pub discogs: DiscogsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MusicBrainzConfig {
    pub contact_email: Option<String>,
    pub rate_limit_per_second: u16,
}

impl Default for MusicBrainzConfig {
    fn default() -> Self {
        Self {
            contact_email: None,
            rate_limit_per_second: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscogsConfig {
    pub personal_access_token: Option<String>,
    pub rate_limit_per_second: u16,
}

impl Default for DiscogsConfig {
    fn default() -> Self {
        Self {
            personal_access_token: None,
            rate_limit_per_second: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub max_concurrent_jobs: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind_address: String,
    pub auth_mode: AuthMode,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".to_string(),
            auth_mode: AuthMode::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMode {
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiConfig {
    pub base_path: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            base_path: "/api".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebConfig {
    pub mount_path: String,
    pub asset_dir: PathBuf,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            mount_path: "/".to_string(),
            asset_dir: PathBuf::from("web"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub field: &'static str,
    pub message: String,
}

impl ConfigError {
    pub fn new(field: &'static str, message: impl Into<String>) -> Self {
        Self {
            field,
            message: message.into(),
        }
    }
}

fn validate_base_path(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if !value.starts_with('/') {
        return Err(ConfigError::new(field, "path must start with '/'"));
    }

    if value.len() > 1 && value.ends_with('/') {
        return Err(ConfigError::new(
            field,
            "path must not end with '/' unless it is the root path",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, ConfigError, UnknownTagPolicy};

    #[test]
    fn default_config_is_valid() {
        assert_eq!(AppConfig::default().validate(), Ok(()));
    }

    #[test]
    fn rejects_api_paths_without_leading_slash() {
        let mut config = AppConfig::default();
        config.api.base_path = "api".to_string();

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "api.base_path",
                "path must start with '/'",
            ))
        );
    }

    #[test]
    fn rejects_non_root_paths_with_trailing_slash() {
        let mut config = AppConfig::default();
        config.web.mount_path = "/ui/".to_string();

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "web.mount_path",
                "path must not end with '/' unless it is the root path",
            ))
        );
    }

    #[test]
    fn rejects_missing_default_export_profile() {
        let mut config = AppConfig::default();
        config.export.default_profile = "roon_compatible".to_string();

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "export.default_profile",
                "default export profile must exist in export.profiles",
            ))
        );
    }

    #[test]
    fn rejects_preserve_selected_without_tag_keys() {
        let mut config = AppConfig::default();
        config.export.tagging.unknown_tag_policy = UnknownTagPolicy::PreserveSelected;
        config.export.tagging.selected_tag_keys.clear();

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "export.tagging.selected_tag_keys",
                "preserve_selected requires at least one selected tag key",
            ))
        );
    }

    #[test]
    fn rejects_zero_worker_concurrency() {
        let mut config = AppConfig::default();
        config.workers.max_concurrent_jobs = 0;

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "workers.max_concurrent_jobs",
                "worker concurrency must be greater than zero",
            ))
        );
    }
}
