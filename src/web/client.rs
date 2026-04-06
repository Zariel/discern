#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebApiClient {
    pub base_path: String,
    pub paths: WebApiPaths,
}

impl WebApiClient {
    pub fn from_api_base_path(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
            paths: WebApiPaths::from_api_base_path(base_path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebApiPaths {
    pub releases: String,
    pub release_groups: String,
    pub release_instances: String,
    pub issues: String,
    pub jobs: String,
    pub import_batches: String,
    pub config_validation: String,
}

impl WebApiPaths {
    pub fn from_api_base_path(base_path: &str) -> Self {
        Self {
            releases: format!("{base_path}/releases"),
            release_groups: format!("{base_path}/release-groups"),
            release_instances: format!("{base_path}/release-instances"),
            issues: format!("{base_path}/issues"),
            jobs: format!("{base_path}/jobs"),
            import_batches: format!("{base_path}/import-batches"),
            config_validation: format!("{base_path}/config/validation"),
        }
    }

    pub fn release(&self, release_id: &str) -> String {
        format!("{}/{}", self.releases, release_id)
    }

    pub fn release_instance(&self, release_instance_id: &str) -> String {
        format!("{}/{}", self.release_instances, release_instance_id)
    }

    pub fn release_instance_export_preview(&self, release_instance_id: &str) -> String {
        format!(
            "{}/{}/export-preview",
            self.release_instances, release_instance_id
        )
    }

    pub fn candidate_matches(&self, release_instance_id: &str) -> String {
        format!(
            "{}/{}/candidate-matches",
            self.release_instances, release_instance_id
        )
    }

    pub fn issue(&self, issue_id: &str) -> String {
        format!("{}/{}", self.issues, issue_id)
    }

    pub fn job(&self, job_id: &str) -> String {
        format!("{}/{}", self.jobs, job_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_client_derives_core_resource_paths() {
        let client = WebApiClient::from_api_base_path("/api");

        assert_eq!(client.paths.releases, "/api/releases");
        assert_eq!(client.paths.issues, "/api/issues");
        assert_eq!(
            client.paths.release_instance_export_preview("relinst_123"),
            "/api/release-instances/relinst_123/export-preview"
        );
        assert_eq!(
            client.paths.candidate_matches("relinst_123"),
            "/api/release-instances/relinst_123/candidate-matches"
        );
    }
}
