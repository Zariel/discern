use serde::{Deserialize, Serialize};

use crate::api::envelope::ApiEnvelope;
use crate::application::config::ValidatedRuntimeConfig;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDiagnosticResource {
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValidationResource {
    pub valid: bool,
    pub diagnostics: Vec<ConfigDiagnosticResource>,
}

pub struct DiagnosticsApi {
    config: ValidatedRuntimeConfig,
}

impl DiagnosticsApi {
    pub fn new(config: ValidatedRuntimeConfig) -> Self {
        Self { config }
    }

    pub fn get_config_validation(
        &self,
        request_id: impl Into<String>,
    ) -> ApiEnvelope<ConfigValidationResource> {
        let request_id = request_id.into();
        ApiEnvelope::success(
            ConfigValidationResource {
                valid: self.config.diagnostics.is_empty(),
                diagnostics: self
                    .config
                    .diagnostics
                    .issues
                    .iter()
                    .map(|diagnostic| ConfigDiagnosticResource {
                        field: diagnostic.field.clone(),
                        message: diagnostic.message.clone(),
                    })
                    .collect(),
            },
            request_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::application::config::{ConfigDiagnostic, ConfigDiagnostics, ValidatedRuntimeConfig};

    use super::*;

    #[test]
    fn config_validation_returns_diagnostics() {
        let mut config =
            ValidatedRuntimeConfig::from_validated_app_config(&crate::config::AppConfig::default());
        config.diagnostics = ConfigDiagnostics {
            issues: vec![ConfigDiagnostic {
                field: "export.default_profile".to_string(),
                message: "profile was missing".to_string(),
            }],
        };

        let envelope = DiagnosticsApi::new(config).get_config_validation("req_config");

        let data = envelope.data.expect("data should exist");
        assert!(!data.valid);
        assert_eq!(data.diagnostics.len(), 1);
        assert_eq!(data.diagnostics[0].field, "export.default_profile");
    }
}
