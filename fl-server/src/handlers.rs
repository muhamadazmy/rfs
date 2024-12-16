use anyhow::Error;
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Extension, Json,
};
use axum_macros::debug_handler;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{mpsc, Arc},
};

use bollard::auth::DockerCredentials;
use serde::{Deserialize, Serialize};

use crate::{
    auth::{SignInBody, SignInResponse, __path_sign_in_handler},
    response::{DirListTemplate, DirLister, ErrorTemplate, TemplateErr},
};
use crate::{
    config::{self, Job},
    response::{FileInfo, ResponseError, ResponseResult},
    serve_flists::visit_dir_one_level,
};
use rfs::fungi::{Reader, Writer};
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(health_check_handler, create_flist_handler, get_flist_state_handler, preview_flist_handler, list_flists_handler, sign_in_handler),
    components(schemas(DirListTemplate, DirLister, FlistBody, Job, ResponseError, ErrorTemplate, TemplateErr, ResponseResult, FileInfo, SignInBody, FlistState, SignInResponse, FlistStateInfo, PreviewResponse)),
    tags(
        (name = "fl-server", description = "Flist conversion API")
    )
)]
pub struct FlistApi;

#[derive(Debug, Deserialize, Serialize, Clone, ToSchema)]
pub struct FlistBody {
    #[schema(example = "redis")]
    pub image_name: String,

    pub username: Option<String>,
    pub password: Option<String>,
    pub auth: Option<String>,
    pub email: Option<String>,
    pub server_address: Option<String>,
    pub identity_token: Option<String>,
    pub registry_token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, ToSchema)]
pub struct PreviewResponse {
    pub content: Vec<PathBuf>,
    pub metadata: String,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub enum FlistState {
    Accepted(String),
    Started(String),
    InProgress(FlistStateInfo),
    Created(String),
    Failed,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct FlistStateInfo {
    msg: String,
    progress: f32,
}

const DEFAULT_LIMIT: usize = 10;
const DEFAULT_PAGE: usize = 1;

#[derive(Deserialize)]
pub struct Pagination {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Deserialize, Clone)]
pub struct Filter {
    pub max_size: Option<usize>,
    pub min_size: Option<usize>,
    username: Option<String>,
    pub name: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/api",
    responses(
        (status = 200, description = "flist server is working", body = String)
    )
)]
pub async fn health_check_handler() -> ResponseResult {
    ResponseResult::Health
}

#[utoipa::path(
    post,
    path = "/v1/api/fl",
    request_body = FlistBody,
    responses(
        (status = 201, description = "Flist conversion started", body = Job),
        (status = 401, description = "Unauthorized user"),
        (status = 403, description = "Forbidden"),
        (status = 409, description = "Conflict"),
        (status = 500, description = "Internal server error"),
    )
)]
#[debug_handler]
pub async fn create_flist_handler(
    State(state): State<Arc<config::AppState>>,
    Extension(username): Extension<String>,
    Json(body): Json<FlistBody>,
) -> impl IntoResponse {
    let cfg = state.config.clone();
    let credentials = Some(DockerCredentials {
        username: body.username,
        password: body.password,
        auth: body.auth,
        email: body.email,
        serveraddress: body.server_address,
        identitytoken: body.identity_token,
        registrytoken: body.registry_token,
    });

    let mut docker_image = body.image_name.to_string();
    if !docker_image.contains(':') {
        docker_image.push_str(":latest");
    }

    let fl_name = docker_image.replace([':', '/'], "-") + ".fl";
    let username_dir = std::path::Path::new(&cfg.flist_dir).join(&username);
    let fl_path = username_dir.join(&fl_name);

    if fl_path.exists() {
        return Err(ResponseError::Conflict("flist already exists".to_string()));
    }

    if let Err(err) = fs::create_dir_all(&username_dir) {
        log::error!(
            "failed to create user flist directory `{:?}` with error {:?}",
            &username_dir,
            err
        );
        return Err(ResponseError::InternalServerError);
    }

    let meta = match Writer::new(&fl_path, true).await {
        Ok(writer) => writer,
        Err(err) => {
            log::error!(
                "failed to create a new writer for flist `{:?}` with error {}",
                fl_path,
                err
            );
            return Err(ResponseError::InternalServerError);
        }
    };

    let store = match rfs::store::parse_router(&cfg.store_url).await {
        Ok(s) => s,
        Err(err) => {
            log::error!("failed to parse router for store with error {}", err);
            return Err(ResponseError::InternalServerError);
        }
    };

    // Create a new job id for the flist request
    let job: Job = Job {
        id: Uuid::new_v4().to_string(),
    };
    let current_job = job.clone();

    state
        .jobs_state
        .lock()
        .expect("failed to lock state")
        .insert(
            job.id.clone(),
            FlistState::Accepted(format!("flist '{}' is accepted", &fl_name)),
        );

    let flist_download_url = std::path::Path::new(&format!("{}:{}", cfg.host, cfg.port))
        .join(cfg.flist_dir)
        .join(username)
        .join(&fl_name);

    tokio::spawn(async move {
        state
            .jobs_state
            .lock()
            .expect("failed to lock state")
            .insert(
                job.id.clone(),
                FlistState::Started(format!("flist '{}' is started", fl_name)),
            );

        let container_name = Uuid::new_v4().to_string();
        let docker_tmp_dir =
            tempdir::TempDir::new(&container_name).expect("failed to create tmp dir for docker");

        let (tx, rx) = mpsc::channel();
        let mut docker_to_fl =
            docker2fl::DockerImageToFlist::new(meta, docker_image, credentials, docker_tmp_dir);

        let res = docker_to_fl.prepare().await;
        if res.is_err() {
            let _ = tokio::fs::remove_file(&fl_path).await;
            state
                .jobs_state
                .lock()
                .expect("failed to lock state")
                .insert(job.id.clone(), FlistState::Failed);
            return;
        }

        let files_count = docker_to_fl.files_count();
        let st = state.clone();
        let job_id = job.id.clone();
        let cloned_fl_path = fl_path.clone();
        tokio::spawn(async move {
            let mut progress: f32 = 0.0;

            for _ in 0..files_count - 1 {
                let step = rx.recv().expect("failed to receive progress") as f32;
                progress += step;
                let progress_percentage = progress / files_count as f32 * 100.0;
                st.jobs_state.lock().expect("failed to lock state").insert(
                    job_id.clone(),
                    FlistState::InProgress(FlistStateInfo {
                        msg: "flist is in progress".to_string(),
                        progress: progress_percentage,
                    }),
                );
                st.flists_progress
                    .lock()
                    .expect("failed to lock state")
                    .insert(cloned_fl_path.clone(), progress_percentage);
            }
        });

        let res = docker_to_fl.pack(store, Some(tx)).await;

        // remove the file created with the writer if fl creation failed
        if res.is_err() {
            log::error!("failed creation failed with error {:?}", res.err());
            let _ = tokio::fs::remove_file(&fl_path).await;
            state
                .jobs_state
                .lock()
                .expect("failed to lock state")
                .insert(job.id.clone(), FlistState::Failed);
            return;
        }

        state
            .jobs_state
            .lock()
            .expect("failed to lock state")
            .insert(
                job.id.clone(),
                FlistState::Created(format!(
                    "flist {:?} is created successfully",
                    flist_download_url
                )),
            );
        state
            .flists_progress
            .lock()
            .expect("failed to lock state")
            .insert(fl_path, 100.0);
    });

    Ok(ResponseResult::FlistCreated(current_job))
}

#[utoipa::path(
    get,
    path = "/v1/api/fl/{job_id}",
    responses(
        (status = 200, description = "Flist state", body = FlistState),
        (status = 404, description = "Flist not found"),
        (status = 500, description = "Internal server error"),
        (status = 401, description = "Unauthorized user"),
        (status = 403, description = "Forbidden"),
    ),
    params(
        ("job_id" = String, Path, description = "flist job id")
    )
)]
#[debug_handler]
pub async fn get_flist_state_handler(
    Path(flist_job_id): Path<String>,
    State(state): State<Arc<config::AppState>>,
) -> impl IntoResponse {
    if !&state
        .jobs_state
        .lock()
        .expect("failed to lock state")
        .contains_key(&flist_job_id.clone())
    {
        return Err(ResponseError::NotFound("flist doesn't exist".to_string()));
    }

    let res_state = state
        .jobs_state
        .lock()
        .expect("failed to lock state")
        .get(&flist_job_id.clone())
        .expect("failed to get from state")
        .to_owned();

    match res_state {
        FlistState::Accepted(_) => Ok(ResponseResult::FlistState(res_state)),
        FlistState::Started(_) => Ok(ResponseResult::FlistState(res_state)),
        FlistState::InProgress(_) => Ok(ResponseResult::FlistState(res_state)),
        FlistState::Created(_) => {
            state
                .jobs_state
                .lock()
                .expect("failed to lock state")
                .remove(&flist_job_id.clone());

            Ok(ResponseResult::FlistState(res_state))
        }
        FlistState::Failed => {
            state
                .jobs_state
                .lock()
                .expect("failed to lock state")
                .remove(&flist_job_id.clone());

            Err(ResponseError::InternalServerError)
        }
    }
}

#[utoipa::path(
	get,
	path = "/v1/api/fl",
	responses(
        (status = 200, description = "Listing flists", body = HashMap<String, Vec<FileInfo>>),
        (status = 401, description = "Unauthorized user"),
        (status = 403, description = "Forbidden"),
        (status = 500, description = "Internal server error"),
	)
)]
#[debug_handler]
pub async fn list_flists_handler(
    State(state): State<Arc<config::AppState>>,
    pagination: Query<Pagination>,
    filter: Query<Filter>,
) -> impl IntoResponse {
    let mut flists: HashMap<String, Vec<FileInfo>> = HashMap::new();

    let pagination: Pagination = pagination.0;
    let page = pagination.page.unwrap_or(DEFAULT_PAGE);
    let limit = pagination.limit.unwrap_or(DEFAULT_LIMIT);

    if page == 0 {
        return Err(ResponseError::BadRequest(
            "requested page should be nonzero positive number".to_string(),
        ));
    }

    let filter: Filter = filter.0;

    let rs: Result<Vec<FileInfo>, std::io::Error> =
        visit_dir_one_level(&state.config.flist_dir, &state, None).await;

    let files = match rs {
        Ok(files) => files,
        Err(e) => {
            log::error!("failed to list flists directory with error: {}", e);
            return Err(ResponseError::InternalServerError);
        }
    };

    for file in files {
        if !file.is_file {
            let flists_per_username =
                visit_dir_one_level(&file.path_uri, &state, Some(filter.clone())).await;

            if let Some(ref filter_username) = filter.username {
                if filter_username.clone() != file.name {
                    continue;
                }
            }

            match flists_per_username {
                Ok(files) => {
                    let username = file.name;
                    flists.insert(username.clone(), Vec::new());

                    let start = limit * (page - 1);
                    let end = limit * page;
                    if files.len() > start {
                        if files.len() >= end {
                            flists.insert(username, files[start..end].to_vec());
                        } else {
                            flists.insert(username, files[start..].to_vec());
                        }
                    }
                }
                Err(e) => {
                    log::error!("failed to list flists per username with error: {}", e);
                    return Err(ResponseError::InternalServerError);
                }
            };
        };
    }

    Ok(ResponseResult::Flists(flists))
}

#[utoipa::path(
	get,
	path = "/v1/api/fl/preview/{flist_path}",
	responses(
        (status = 200, description = "Flist preview result", body = PreviewResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized user"),
        (status = 403, description = "Forbidden"),
        (status = 500, description = "Internal server error"),
	),
    params(
        ("flist_path" = String, Path, description = "flist file path")
    )
)]
#[debug_handler]
pub async fn preview_flist_handler(
    State(state): State<Arc<config::AppState>>,
    Path(flist_path): Path<String>,
) -> impl IntoResponse {
    let fl_path = flist_path;

    match validate_flist_path(&state, &fl_path).await {
        Ok(_) => (),
        Err(err) => return Err(ResponseError::BadRequest(err.to_string())),
    };

    let content = match get_flist_content(&fl_path).await {
        Ok(paths) => paths,
        Err(_) => return Err(ResponseError::InternalServerError),
    };

    let bytes = match std::fs::read(&fl_path) {
        Ok(b) => b,
        Err(err) => {
            log::error!(
                "failed to read flist '{}' into bytes with error {}",
                fl_path,
                err
            );
            return Err(ResponseError::InternalServerError);
        }
    };

    Ok(ResponseResult::PreviewFlist(PreviewResponse {
        content,
        metadata: state.config.store_url.join("-"),
        checksum: sha256::digest(&bytes),
    }))
}

async fn validate_flist_path(state: &Arc<config::AppState>, fl_path: &String) -> Result<(), Error> {
    // validate path starting with `/`
    if fl_path.starts_with("/") {
        anyhow::bail!("invalid flist path '{}', shouldn't start with '/'", fl_path);
    }

    // path should include 3 parts [parent dir, username, flist file]
    let parts: Vec<_> = fl_path.split("/").collect();
    if parts.len() != 3 {
        anyhow::bail!(
            format!("invalid flist path '{}', should consist of 3 parts [parent directory, username and flist name", fl_path
        ));
    }

    // validate parent dir
    if parts[0] != state.config.flist_dir {
        anyhow::bail!(
            "invalid flist path '{}', parent directory should be '{}'",
            fl_path,
            state.config.flist_dir
        );
    }

    // validate username
    match state.db.get_user_by_username(&parts[1]) {
        Some(_) => (),
        None => {
            anyhow::bail!(
                "invalid flist path '{}', username '{}' doesn't exist",
                fl_path,
                parts[1]
            );
        }
    };

    // validate flist extension
    let fl_name = parts[2].to_string();
    let ext = match std::path::Path::new(&fl_name).extension() {
        Some(ex) => ex.to_string_lossy().to_string(),
        None => "".to_string(),
    };

    if ext != "fl" {
        anyhow::bail!(
            "invalid flist path '{}', invalid flist extension '{}' should be 'fl'",
            fl_path,
            ext
        );
    }

    // validate flist existence
    if !std::path::Path::new(parts[0])
        .join(parts[1])
        .join(&fl_name)
        .exists()
    {
        anyhow::bail!("flist '{}' doesn't exist", fl_path);
    }

    Ok(())
}

async fn get_flist_content(fl_path: &String) -> Result<Vec<PathBuf>, Error> {
    let mut visitor = ReadVisitor::default();

    let meta = match Reader::new(&fl_path).await {
        Ok(reader) => reader,
        Err(err) => {
            log::error!(
                "failed to initialize metadata database for flist `{}` with error {}",
                fl_path,
                err
            );
            anyhow::bail!("Internal server error");
        }
    };

    match meta.walk(&mut visitor).await {
        Ok(()) => return Ok(visitor.into_inner()),
        Err(err) => {
            log::error!(
                "failed to walk through metadata for flist `{}` with error {}",
                fl_path,
                err
            );
            anyhow::bail!("Internal server error");
        }
    };
}

#[derive(Default)]
struct ReadVisitor {
    inner: Vec<PathBuf>,
}

impl ReadVisitor {
    pub fn into_inner(self) -> Vec<PathBuf> {
        self.inner
    }
}

#[async_trait::async_trait]
impl rfs::fungi::meta::WalkVisitor for ReadVisitor {
    async fn visit(
        &mut self,
        path: &std::path::Path,
        _node: &rfs::fungi::meta::Inode,
    ) -> rfs::fungi::meta::Result<rfs::fungi::meta::Walk> {
        self.inner.push(path.to_path_buf());
        Ok(rfs::fungi::meta::Walk::Continue)
    }
}
