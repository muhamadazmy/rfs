use std::collections::HashMap;

use askama::Template;
use axum::{
    body::Body,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    Json,
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    auth::SignInResponse,
    config::Job,
    handlers::{FlistState, PreviewResponse},
};

#[derive(Serialize, ToSchema)]
pub enum ResponseError {
    InternalServerError,
    Conflict(String),
    NotFound(String),
    Unauthorized(String),
    BadRequest(String),
    Forbidden(String),
    TemplateError(ErrorTemplate),
}

impl IntoResponse for ResponseError {
    fn into_response(self) -> Response<Body> {
        match self {
            ResponseError::InternalServerError => {
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
            }
            ResponseError::Conflict(msg) => (StatusCode::CONFLICT, msg).into_response(),
            ResponseError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
            ResponseError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg).into_response(),
            ResponseError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            ResponseError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg).into_response(),
            ResponseError::TemplateError(t) => match t.render() {
                Ok(html) => {
                    let mut resp = Html(html).into_response();
                    match t.err {
                        TemplateErr::NotFound(reason) => {
                            *resp.status_mut() = StatusCode::NOT_FOUND;
                            resp.headers_mut()
                                .insert(FAIL_REASON_HEADER_NAME, reason.parse().unwrap());
                        }
                        TemplateErr::BadRequest(reason) => {
                            *resp.status_mut() = StatusCode::BAD_REQUEST;
                            resp.headers_mut()
                                .insert(FAIL_REASON_HEADER_NAME, reason.parse().unwrap());
                        }
                        TemplateErr::InternalServerError(reason) => {
                            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            resp.headers_mut()
                                .insert(FAIL_REASON_HEADER_NAME, reason.parse().unwrap());
                        }
                    }
                    resp
                }
                Err(err) => {
                    tracing::error!("template render failed, err={}", err);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to render template. Error: {}", err),
                    )
                        .into_response()
                }
            },
        }
    }
}

#[derive(ToSchema)]
pub enum ResponseResult {
    Health,
    FlistCreated(Job),
    FlistState(FlistState),
    Flists(HashMap<String, Vec<FileInfo>>),
    PreviewFlist(PreviewResponse),
    SignedIn(SignInResponse),
    DirTemplate(DirListTemplate),
    Res(hyper::Response<tower_http::services::fs::ServeFileSystemResponseBody>),
}

impl IntoResponse for ResponseResult {
    fn into_response(self) -> Response<Body> {
        match self {
            ResponseResult::Health => (
                StatusCode::OK,
                Json(serde_json::json!({"msg": "flist server is working"})),
            )
                .into_response(),
            ResponseResult::SignedIn(token) => (StatusCode::CREATED, Json(token)).into_response(),
            ResponseResult::FlistCreated(job) => (StatusCode::CREATED, Json(job)).into_response(),
            ResponseResult::FlistState(flist_state) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "flist_state": flist_state
                })),
            )
                .into_response(),
            ResponseResult::Flists(flists) => (StatusCode::OK, Json(flists)).into_response(),
            ResponseResult::PreviewFlist(content) => {
                (StatusCode::OK, Json(content)).into_response()
            }
            ResponseResult::DirTemplate(t) => match t.render() {
                Ok(html) => Html(html).into_response(),
                Err(err) => {
                    tracing::error!("template render failed, err={}", err);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to render template. Error: {}", err),
                    )
                        .into_response()
                }
            },
            ResponseResult::Res(res) => res.map(axum::body::Body::new),
        }
    }
}

//////// TEMPLATES ////////

#[derive(Serialize, Clone, Debug, ToSchema)]
pub struct FileInfo {
    pub name: String,
    pub path_uri: String,
    pub is_file: bool,
    pub size: u64,
    pub last_modified: i64,
    pub progress: f32,
}

#[derive(Serialize, ToSchema)]
pub struct DirLister {
    pub files: Vec<FileInfo>,
}

#[derive(Template, Serialize, ToSchema)]
#[template(path = "index.html")]
pub struct DirListTemplate {
    pub lister: DirLister,
    pub cur_path: String,
}

mod filters {
    pub(crate) fn datetime(ts: &i64) -> ::askama::Result<String> {
        if let Ok(format) =
            time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second] UTC")
        {
            return Ok(time::OffsetDateTime::from_unix_timestamp(*ts)
                .unwrap()
                .format(&format)
                .unwrap());
        }
        Err(askama::Error::Fmt(std::fmt::Error))
    }
}

#[derive(Template, Serialize, ToSchema)]
#[template(path = "error.html")]
pub struct ErrorTemplate {
    pub err: TemplateErr,
    pub cur_path: String,
    pub message: String,
}

const FAIL_REASON_HEADER_NAME: &str = "fl-server-fail-reason";

#[derive(Serialize, ToSchema)]
pub enum TemplateErr {
    BadRequest(String),
    NotFound(String),
    InternalServerError(String),
}
