#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppConfig {
    pub storage: StorageConfig,
    pub api: ApiConfig,
    pub web: WebConfig,
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_base_path("api.base_path", &self.api.base_path)?;
        validate_base_path("web.mount_path", &self.web.mount_path)?;

        if self.web.asset_dir.trim().is_empty() {
            return Err(ConfigError::new(
                "web.asset_dir",
                "web asset directory must not be empty",
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub sqlite_path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_path: "discern.db".to_string(),
        }
    }
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
    pub asset_dir: String,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            mount_path: "/".to_string(),
            asset_dir: "web".to_string(),
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
    use super::{AppConfig, ConfigError};

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
}
