pub mod envelope;
pub mod error;
pub mod ingest;
pub mod jobs;
pub mod pagination;
pub mod routes;

use crate::config::ApiConfig;

pub use envelope::{ApiEnvelope, ApiMeta};
pub use error::{ApiError, ApiErrorCode};
pub use ingest::IngestApi;
pub use jobs::JobsApi;
pub use pagination::ApiPaginationMeta;
pub use routes::{ApiRoute, HttpMethod};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiSurface {
    pub base_path: String,
    pub routes: Vec<ApiRoute>,
}

impl ApiSurface {
    pub fn from_config(config: &ApiConfig) -> Self {
        Self {
            base_path: config.base_path.clone(),
            routes: routes::core_routes(&config.base_path),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::ApiConfig;
    use crate::support::pagination::{Page, PageRequest};

    use super::*;

    #[test]
    fn success_envelope_serializes_with_pagination_meta() {
        let page = Page {
            items: vec!["job-1".to_string(), "job-2".to_string()],
            request: PageRequest::new(2, 0),
            total: 5,
        };
        let envelope = ApiEnvelope::success_with_pagination(
            page.items.clone(),
            "req_123",
            ApiPaginationMeta::from_page(&page),
        );

        let value = serde_json::to_value(envelope).expect("envelope should serialize");
        assert_eq!(
            value,
            json!({
                "data": ["job-1", "job-2"],
                "error": null,
                "meta": {
                    "request_id": "req_123",
                    "pagination": {
                        "limit": 2,
                        "offset": 0,
                        "total": 5,
                        "has_more": true,
                        "next_offset": 2,
                        "next_cursor": null
                    }
                }
            })
        );
    }

    #[test]
    fn error_envelope_serializes_null_data_and_error_details() {
        let envelope = ApiEnvelope::error(
            ApiError::new(
                ApiErrorCode::Conflict,
                "Multiple candidate releases matched with similar confidence",
                Some(json!({
                    "release_instance_id": "relinst_123"
                })),
            ),
            "req_456",
        );

        let value = serde_json::to_value(envelope).expect("error should serialize");
        assert_eq!(
            value,
            json!({
                "data": null,
                "error": {
                    "code": "conflict",
                    "message": "Multiple candidate releases matched with similar confidence",
                    "details": {
                        "release_instance_id": "relinst_123"
                    }
                },
                "meta": {
                    "request_id": "req_456",
                    "pagination": null
                }
            })
        );
    }

    #[test]
    fn pagination_meta_uses_offset_convention() {
        let page = Page {
            items: vec![1, 2],
            request: PageRequest::new(2, 4),
            total: 8,
        };

        let pagination = ApiPaginationMeta::from_page(&page);

        assert_eq!(
            pagination,
            ApiPaginationMeta {
                limit: 2,
                offset: 4,
                total: 8,
                has_more: true,
                next_offset: Some(6),
                next_cursor: None,
            }
        );
    }

    #[test]
    fn api_surface_scaffolds_core_tdd_routes() {
        let surface = ApiSurface::from_config(&ApiConfig::default());

        assert!(
            surface
                .routes
                .iter()
                .any(|route| { route.method == HttpMethod::Get && route.path == "/api/jobs" })
        );
        assert!(surface.routes.iter().any(|route| {
            route.method == HttpMethod::Post
                && route.path == "/api/import-batches"
                && route.long_running
        }));
        assert!(surface.routes.iter().any(|route| {
            route.method == HttpMethod::Get
                && route.path == "/api/release-instances/{id}/export-preview"
        }));
    }
}
