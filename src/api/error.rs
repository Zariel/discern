use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub details: Option<Value>,
}

impl ApiError {
    pub fn new(code: ApiErrorCode, message: impl Into<String>, details: Option<Value>) -> Self {
        Self {
            code: code.as_str().to_string(),
            message: message.into(),
            details,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiErrorCode {
    InvalidRequest,
    ValidationFailed,
    NotFound,
    Conflict,
    UnsupportedOperation,
    InternalError,
}

impl ApiErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ApiErrorCode::InvalidRequest => "invalid_request",
            ApiErrorCode::ValidationFailed => "validation_failed",
            ApiErrorCode::NotFound => "not_found",
            ApiErrorCode::Conflict => "conflict",
            ApiErrorCode::UnsupportedOperation => "unsupported_operation",
            ApiErrorCode::InternalError => "internal_error",
        }
    }
}
