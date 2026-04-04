use crate::config::WebConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSurface {
    pub mount_path: String,
    pub asset_dir: String,
}

impl WebSurface {
    pub fn from_config(config: &WebConfig) -> Self {
        Self {
            mount_path: config.mount_path.clone(),
            asset_dir: config.asset_dir.clone(),
        }
    }
}
