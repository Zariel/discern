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

        if self.workers.file_io_concurrency == 0 {
            return Err(ConfigError::new(
                "workers.file_io_concurrency",
                "file I/O worker concurrency must be greater than zero",
            ));
        }

        if self.workers.provider_request_concurrency == 0 {
            return Err(ConfigError::new(
                "workers.provider_request_concurrency",
                "provider request concurrency must be greater than zero",
            ));
        }

        if self.workers.db_write_concurrency == 0 {
            return Err(ConfigError::new(
                "workers.db_write_concurrency",
                "DB write concurrency must be greater than zero",
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

    pub fn validate_startup(&self) -> Result<(), ConfigValidationReport> {
        let mut report = ConfigValidationReport::default();

        if let Err(error) = self.validate() {
            report.push(error.field, error.message);
        }

        let release_placeholders = validate_template(
            &mut report,
            "export.path_templates.release_template",
            &self.export.path_templates.release_template,
        );
        let release_instance_placeholders = validate_template(
            &mut report,
            "export.path_templates.release_instance_template",
            &self.export.path_templates.release_instance_template,
        );

        validate_watch_directories(self, &mut report);
        validate_provider_credentials(self, &mut report);
        validate_worker_topology(self, &mut report);
        validate_distinguishability_rules(
            self,
            &release_placeholders,
            &release_instance_placeholders,
            &mut report,
        );

        report.into_result()
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
            release_instance_template:
                "{release_year} - {edition_label}/{format_family}-{bitrate_mode}-{bitrate_kbps}-{sample_rate_hz}-{bit_depth}/{source_name}".to_string(),
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
    pub enabled: bool,
    pub personal_access_token: Option<String>,
    pub rate_limit_per_second: u16,
}

impl Default for DiscogsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            personal_access_token: None,
            rate_limit_per_second: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub max_concurrent_jobs: usize,
    pub file_io_concurrency: usize,
    pub provider_request_concurrency: usize,
    pub db_write_concurrency: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: 2,
            file_io_concurrency: 2,
            provider_request_concurrency: 2,
            db_write_concurrency: 1,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigValidationReport {
    pub errors: Vec<ConfigValidationIssue>,
}

impl ConfigValidationReport {
    pub fn push(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.errors.push(ConfigValidationIssue {
            field: field.into(),
            message: message.into(),
        });
    }

    pub fn into_result(self) -> Result<(), Self> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigValidationIssue {
    pub field: String,
    pub message: String,
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

fn validate_template(
    report: &mut ConfigValidationReport,
    field: &str,
    template: &str,
) -> Vec<String> {
    let mut placeholders = Vec::new();
    let mut chars = template.char_indices().peekable();

    while let Some((index, current)) = chars.next() {
        match current {
            '{' => {
                let placeholder_start = index + 1;
                let mut closing_index = None;

                for (candidate_index, candidate) in chars.by_ref() {
                    if candidate == '}' {
                        closing_index = Some(candidate_index);
                        break;
                    }

                    if candidate == '{' {
                        report.push(field, "path template contains nested '{'");
                        return placeholders;
                    }
                }

                let Some(closing_index) = closing_index else {
                    report.push(field, "path template contains an unclosed placeholder");
                    return placeholders;
                };

                let placeholder = &template[placeholder_start..closing_index];

                if !is_known_template_placeholder(placeholder) {
                    report.push(
                        field,
                        format!("path template uses unknown placeholder '{placeholder}'"),
                    );
                    continue;
                }

                placeholders.push(placeholder.to_string());
            }
            '}' => {
                report.push(field, "path template contains an unmatched '}'");
                return placeholders;
            }
            _ => {}
        }
    }

    placeholders
}

fn is_known_template_placeholder(placeholder: &str) -> bool {
    matches!(
        placeholder,
        "album_artist"
            | "release_title"
            | "release_year"
            | "edition_label"
            | "format_family"
            | "bitrate_mode"
            | "bitrate_kbps"
            | "sample_rate_hz"
            | "bit_depth"
            | "source_name"
    )
}

fn validate_watch_directories(config: &AppConfig, report: &mut ConfigValidationReport) {
    let managed_library_root = normalize_path(&config.storage.managed_library_root);
    let mut normalized_watch_paths = Vec::new();

    for (index, watcher) in config.storage.watch_directories.iter().enumerate() {
        let normalized = normalize_path(&watcher.path);
        let field = format!("storage.watch_directories[{index}].path");

        if paths_overlap(&managed_library_root, &normalized) {
            report.push(
                field.clone(),
                "watch directory must not overlap the managed library root",
            );
        }

        if matches!(watcher.import_mode_override, Some(ImportMode::Move))
            && paths_overlap(&managed_library_root, &normalized)
        {
            report.push(
                format!("storage.watch_directories[{index}].import_mode_override"),
                "move mode is not supported when a watch directory overlaps the managed library root",
            );
        }

        normalized_watch_paths.push((field, normalized));
    }

    for left_index in 0..normalized_watch_paths.len() {
        for right_index in (left_index + 1)..normalized_watch_paths.len() {
            let (left_field, left_path) = &normalized_watch_paths[left_index];
            let (right_field, right_path) = &normalized_watch_paths[right_index];

            if paths_overlap(left_path, right_path) {
                report.push(
                    left_field.clone(),
                    format!("watch directory overlaps with {right_field}"),
                );
                report.push(
                    right_field.clone(),
                    format!("watch directory overlaps with {left_field}"),
                );
            }
        }
    }
}

fn validate_provider_credentials(config: &AppConfig, report: &mut ConfigValidationReport) {
    if config.providers.discogs.enabled
        && config
            .providers
            .discogs
            .personal_access_token
            .as_deref()
            .is_none_or(str::is_empty)
    {
        report.push(
            "providers.discogs.personal_access_token",
            "discogs requires a personal access token when enabled",
        );
    }
}

fn validate_worker_topology(config: &AppConfig, report: &mut ConfigValidationReport) {
    if config.workers.db_write_concurrency != 1 {
        report.push(
            "workers.db_write_concurrency",
            "SQLite runtime requires a single DB write worker",
        );
    }
}

fn validate_distinguishability_rules(
    config: &AppConfig,
    release_placeholders: &[String],
    release_instance_placeholders: &[String],
    report: &mut ConfigValidationReport,
) {
    let has_release_edition = has_placeholder(release_placeholders, "edition_label");
    let has_instance_edition = has_placeholder(release_instance_placeholders, "edition_label");
    let has_source_name = has_placeholder(release_instance_placeholders, "source_name");
    let has_format_family = has_placeholder(release_instance_placeholders, "format_family");
    let has_quality_detail = has_any_placeholder(
        release_instance_placeholders,
        &[
            "bitrate_mode",
            "bitrate_kbps",
            "sample_rate_hz",
            "bit_depth",
        ],
    );

    for profile in &config.export.profiles {
        if matches!(profile.edition_visibility, EditionVisibilityPolicy::Hidden)
            && !has_release_edition
            && !has_instance_edition
        {
            report.push(
                format!("export.profiles[{}].edition_visibility", profile.name),
                "edition visibility is hidden, but no path template includes {edition_label}",
            );
        }

        if matches!(
            profile.provenance_visibility,
            VariantVisibilityPolicy::Hidden
        ) && config.import.duplicate_policy == DuplicatePolicy::AllowIfDistinguishable
            && !has_source_name
        {
            report.push(
                format!("export.profiles[{}].provenance_visibility", profile.name),
                "duplicate coexistence requires {source_name} in the release instance path when provenance is hidden from tags",
            );
        }

        if !has_format_family || !has_quality_detail {
            report.push(
                format!("export.profiles[{}].technical_visibility", profile.name),
                "release instance paths must include format_family and at least one bitrate or quality placeholder",
            );
        }
    }
}

fn has_placeholder(placeholders: &[String], placeholder: &str) -> bool {
    placeholders
        .iter()
        .any(|candidate| candidate == placeholder)
}

fn has_any_placeholder(placeholders: &[String], wanted: &[&str]) -> bool {
    placeholders
        .iter()
        .any(|candidate| wanted.iter().any(|wanted| candidate == wanted))
}

fn normalize_path(path: &std::path::Path) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }

    normalized
}

fn paths_overlap(left: &std::path::Path, right: &std::path::Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::domain::import_batch::ImportMode;

    use super::{
        AppConfig, ConfigError, ConfigValidationIssue, ConfigValidationReport,
        EditionVisibilityPolicy, UnknownTagPolicy, VariantVisibilityPolicy, WatchDirectoryConfig,
        WatchScanMode,
    };

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

    #[test]
    fn rejects_zero_file_io_worker_concurrency() {
        let mut config = AppConfig::default();
        config.workers.file_io_concurrency = 0;

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "workers.file_io_concurrency",
                "file I/O worker concurrency must be greater than zero",
            ))
        );
    }

    #[test]
    fn rejects_zero_provider_request_concurrency() {
        let mut config = AppConfig::default();
        config.workers.provider_request_concurrency = 0;

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "workers.provider_request_concurrency",
                "provider request concurrency must be greater than zero",
            ))
        );
    }

    #[test]
    fn rejects_zero_db_write_concurrency() {
        let mut config = AppConfig::default();
        config.workers.db_write_concurrency = 0;

        assert_eq!(
            config.validate(),
            Err(ConfigError::new(
                "workers.db_write_concurrency",
                "DB write concurrency must be greater than zero",
            ))
        );
    }

    #[test]
    fn startup_validation_rejects_overlapping_watch_directories() {
        let mut config = AppConfig::default();
        config.storage.watch_directories[0].path = PathBuf::from("/music/incoming");
        config.storage.watch_directories.push(WatchDirectoryConfig {
            name: "nested".to_string(),
            path: PathBuf::from("/music/incoming/subdir"),
            scan_mode: WatchScanMode::EventDriven,
            import_mode_override: None,
        });

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![
                    ConfigValidationIssue {
                        field: "storage.watch_directories[0].path".to_string(),
                        message: "watch directory overlaps with storage.watch_directories[1].path"
                            .to_string(),
                    },
                    ConfigValidationIssue {
                        field: "storage.watch_directories[1].path".to_string(),
                        message: "watch directory overlaps with storage.watch_directories[0].path"
                            .to_string(),
                    },
                ],
            })
        );
    }

    #[test]
    fn startup_validation_rejects_unknown_template_placeholders() {
        let mut config = AppConfig::default();
        config.export.path_templates.release_template =
            "{album_artist}/{unsupported_token}".to_string();

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![ConfigValidationIssue {
                    field: "export.path_templates.release_template".to_string(),
                    message: "path template uses unknown placeholder 'unsupported_token'"
                        .to_string(),
                }],
            })
        );
    }

    #[test]
    fn startup_validation_rejects_hidden_edition_without_path_qualifier() {
        let mut config = AppConfig::default();
        config.export.profiles[0].edition_visibility = EditionVisibilityPolicy::Hidden;
        config.export.path_templates.release_instance_template =
            "{release_year}/{format_family}-{bitrate_mode}-{bitrate_kbps}/{source_name}"
                .to_string();

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![ConfigValidationIssue {
                    field: "export.profiles[generic_player].edition_visibility".to_string(),
                    message:
                        "edition visibility is hidden, but no path template includes {edition_label}"
                            .to_string(),
                }],
            })
        );
    }

    #[test]
    fn startup_validation_rejects_duplicate_allowance_without_source_qualifier() {
        let mut config = AppConfig::default();
        config.export.path_templates.release_instance_template =
            "{release_year} - {edition_label}/{format_family}-{bitrate_mode}-{bitrate_kbps}"
                .to_string();
        config.export.profiles[0].provenance_visibility = VariantVisibilityPolicy::Hidden;

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![ConfigValidationIssue {
                    field: "export.profiles[generic_player].provenance_visibility".to_string(),
                    message: "duplicate coexistence requires {source_name} in the release instance path when provenance is hidden from tags".to_string(),
                }],
            })
        );
    }

    #[test]
    fn startup_validation_rejects_enabled_discogs_without_token() {
        let mut config = AppConfig::default();
        config.providers.discogs.enabled = true;

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![ConfigValidationIssue {
                    field: "providers.discogs.personal_access_token".to_string(),
                    message: "discogs requires a personal access token when enabled".to_string(),
                }],
            })
        );
    }

    #[test]
    fn startup_validation_rejects_multiple_db_write_workers() {
        let mut config = AppConfig::default();
        config.workers.db_write_concurrency = 2;

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![ConfigValidationIssue {
                    field: "workers.db_write_concurrency".to_string(),
                    message: "SQLite runtime requires a single DB write worker".to_string(),
                }],
            })
        );
    }

    #[test]
    fn startup_validation_rejects_watch_directory_overlap_with_library_root() {
        let mut config = AppConfig::default();
        config.storage.managed_library_root = PathBuf::from("/music/library");
        config.storage.watch_directories[0].path = PathBuf::from("/music");
        config.storage.watch_directories[0].import_mode_override = Some(ImportMode::Move);

        assert_eq!(
            config.validate_startup(),
            Err(ConfigValidationReport {
                errors: vec![
                    ConfigValidationIssue {
                        field: "storage.watch_directories[0].path".to_string(),
                        message: "watch directory must not overlap the managed library root"
                            .to_string(),
                    },
                    ConfigValidationIssue {
                        field: "storage.watch_directories[0].import_mode_override".to_string(),
                        message: "move mode is not supported when a watch directory overlaps the managed library root".to_string(),
                    },
                ],
            })
        );
    }
}
