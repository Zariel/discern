use serde::{Deserialize, Serialize};

use crate::api::error::ApiError;
use crate::api::pagination::ApiPaginationMeta;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiEnvelope<T> {
    pub data: Option<T>,
    pub error: Option<ApiError>,
    pub meta: ApiMeta,
}

impl<T> ApiEnvelope<T> {
    pub fn success(data: T, request_id: impl Into<String>) -> Self {
        Self {
            data: Some(data),
            error: None,
            meta: ApiMeta::new(request_id, None),
        }
    }

    pub fn success_with_pagination(
        data: T,
        request_id: impl Into<String>,
        pagination: ApiPaginationMeta,
    ) -> Self {
        Self {
            data: Some(data),
            error: None,
            meta: ApiMeta::new(request_id, Some(pagination)),
        }
    }
}

impl ApiEnvelope<()> {
    pub fn error(error: ApiError, request_id: impl Into<String>) -> Self {
        Self {
            data: None,
            error: Some(error),
            meta: ApiMeta::new(request_id, None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiMeta {
    pub request_id: String,
    pub pagination: Option<ApiPaginationMeta>,
}

impl ApiMeta {
    pub fn new(request_id: impl Into<String>, pagination: Option<ApiPaginationMeta>) -> Self {
        Self {
            request_id: request_id.into(),
            pagination,
        }
    }
}
