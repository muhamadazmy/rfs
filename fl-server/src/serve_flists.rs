use axum::extract::State;
use std::{io::Error, path::PathBuf, sync::Arc};
use tokio::io;
use tower::util::ServiceExt;
use tower_http::services::ServeDir;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::IntoResponse,
};
use axum_macros::debug_handler;
use percent_encoding::percent_decode;

use crate::{
    config,
    handlers::Filter,
    response::{
        DirListTemplate, DirLister, ErrorTemplate, FileInfo, ResponseError, ResponseResult,
        TemplateErr,
    },
};

#[debug_handler]
pub async fn serve_flists(
    State(state): State<Arc<config::AppState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    let path = req.uri().path().to_string();

    return match ServeDir::new("").oneshot(req).await {
        Ok(res) => {
            let status = res.status();
            match status {
                StatusCode::NOT_FOUND => {
                    let full_path = match validate_path(&path) {
                        Ok(p) => p,
                        Err(_) => {
                            return Err(ResponseError::TemplateError(ErrorTemplate {
                                err: TemplateErr::BadRequest("invalid path".to_string()),
                                cur_path: path.to_string(),
                                message: "invalid path".to_owned(),
                            }));
                        }
                    };

                    let cur_path = std::path::Path::new(&full_path);

                    match cur_path.is_dir() {
                        true => {
                            let rs = visit_dir_one_level(&full_path, &state, None).await;
                            match rs {
                                Ok(files) => Ok(ResponseResult::DirTemplate(DirListTemplate {
                                    lister: DirLister { files },
                                    cur_path: path.to_string(),
                                })),
                                Err(e) => Err(ResponseError::TemplateError(ErrorTemplate {
                                    err: TemplateErr::InternalServerError(e.to_string()),
                                    cur_path: path.to_string(),
                                    message: e.to_string(),
                                })),
                            }
                        }
                        false => Err(ResponseError::TemplateError(ErrorTemplate {
                            err: TemplateErr::NotFound("file not found".to_string()),
                            cur_path: path.to_string(),
                            message: "file not found".to_owned(),
                        })),
                    }
                }
                _ => Ok(ResponseResult::Res(res)),
            }
        }
        Err(err) => Err(ResponseError::TemplateError(ErrorTemplate {
            err: TemplateErr::InternalServerError(format!("Unhandled error: {}", err)),
            cur_path: path.to_string(),
            message: format!("Unhandled error: {}", err),
        })),
    };
}

fn validate_path(path: &str) -> io::Result<PathBuf> {
    let path = path.trim_start_matches('/');
    let path = percent_decode(path.as_ref()).decode_utf8_lossy();

    let mut full_path = PathBuf::new();

    // validate
    for seg in path.split('/') {
        if seg.starts_with("..") || seg.contains('\\') {
            return Err(Error::other("invalid path"));
        }
        full_path.push(seg);
    }

    Ok(full_path)
}

pub async fn visit_dir_one_level<P: AsRef<std::path::Path>>(
    path: P,
    state: &Arc<config::AppState>,
    filter: Option<Filter>,
) -> io::Result<Vec<FileInfo>> {
    let path = path.as_ref();
    let mut dir = tokio::fs::read_dir(path).await?;
    let mut files: Vec<FileInfo> = Vec::new();

    while let Some(child) = dir.next_entry().await? {
        let path_uri = child.path().to_string_lossy().to_string();
        let is_file = child.file_type().await?.is_file();
        let name = child.file_name().to_string_lossy().to_string();
        let size = child.metadata().await?.len();

        let mut progress = 0.0;
        if is_file {
            match state
                .flists_progress
                .lock()
                .expect("failed to lock state")
                .get(&path.join(&name).to_path_buf())
            {
                Some(p) => progress = *p,
                None => progress = 100.0,
            }

            let ext = child
                .path()
                .extension()
                .expect("failed to get path extension")
                .to_string_lossy()
                .to_string();
            if ext != "fl" {
                continue;
            }
        }

        if let Some(ref filter_files) = filter {
            if let Some(ref filter_name) = filter_files.name {
                if filter_name.clone() != name {
                    continue;
                }
            }

            if let Some(ref filter_max_size) = filter_files.max_size {
                if filter_max_size.clone() < size as usize {
                    continue;
                }
            }

            if let Some(ref filter_min_size) = filter_files.min_size {
                if filter_min_size.clone() > size as usize {
                    continue;
                }
            }
        }

        files.push(FileInfo {
            name,
            path_uri,
            is_file,
            size: size,
            last_modified: child
                .metadata()
                .await?
                .modified()?
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .expect("failed to get duration")
                .as_secs() as i64,
            progress,
        });
    }

    Ok(files)
}
