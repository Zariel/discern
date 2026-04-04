use crate::config::ApiConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiSurface {
    pub base_path: String,
}

impl ApiSurface {
    pub fn from_config(config: &ApiConfig) -> Self {
        Self {
            base_path: config.base_path.clone(),
        }
    }
}
