use crate::response::response::Response;
use axum::{Json, http::StatusCode, response::IntoResponse};
use tracing::error;

pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn internal(message: impl ToString) -> Self {
        let msg = message.to_string();
        error!(status = 500, error = %msg, "internal server error");
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg,
        }
    }

    pub fn bad_request(message: impl ToString) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            message: message.to_string(),
        }
    }

    pub fn not_found(message: impl ToString) -> Self {
        ApiError {
            status: StatusCode::NOT_FOUND,
            message: message.to_string(),
        }
    }

    pub fn forbidden(message: impl ToString) -> Self {
        ApiError {
            status: StatusCode::FORBIDDEN,
            message: message.to_string(),
        }
    }

    pub fn unauthorized(message: impl ToString) -> Self {
        ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: message.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = Response::<()>::error(self.status.as_u16() as i32, self.message);
        (self.status, Json(body)).into_response()
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ApiError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        let msg = e.to_string();
        if msg.contains("not found") {
            ApiError::not_found(msg)
        } else {
            ApiError::internal(msg)
        }
    }
}
