use crate::application::config::ValidatedRuntimeConfig;
use crate::application::observability::ObservabilityContext;
use crate::application::workers::WorkerPools;
use crate::config::AppConfig;

#[derive(Debug, Clone)]
pub struct ApplicationContext {
    pub config: ValidatedRuntimeConfig,
    pub observability: ObservabilityContext,
    pub workers: WorkerPools,
}

impl ApplicationContext {
    pub fn new(config: &AppConfig) -> Self {
        Self {
            config: ValidatedRuntimeConfig::from_validated_app_config(config),
            observability: ObservabilityContext::default(),
            workers: WorkerPools::from_config(&config.workers),
        }
    }
}

impl Default for ApplicationContext {
    fn default() -> Self {
        Self::new(&AppConfig::default())
    }
}
