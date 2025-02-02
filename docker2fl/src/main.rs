use anyhow::Result;
use bollard::auth::DockerCredentials;
use clap::{ArgAction, Parser};
use rfs::fungi;
use rfs::store::parse_router;
use tokio::runtime::Builder;
use uuid::Uuid;

mod docker2fl;

#[derive(Parser, Debug)]
#[clap(name ="docker2fl", author, version = env!("GIT_VERSION"), about, long_about = None)]
struct Options {
    /// enable debugging logs
    #[clap(short, long, action=ArgAction::Count)]
    debug: u8,

    /// store url for rfs in the format [xx-xx=]<url>. the range xx-xx is optional and used for
    /// sharding. the URL is per store type, please check docs for more information
    #[clap(short, long, required = true, action=ArgAction::Append)]
    store: Vec<String>,

    /// name of the docker image to be converted to flist
    #[clap(short, long, required = true)]
    image_name: String,

    // docker credentials
    /// docker hub server username
    #[clap(long, required = false)]
    username: Option<String>,

    /// docker hub server password
    #[clap(long, required = false)]
    password: Option<String>,

    /// docker hub server auth
    #[clap(long, required = false)]
    auth: Option<String>,

    /// docker hub server email
    #[clap(long, required = false)]
    email: Option<String>,

    /// docker hub server address
    #[clap(long, required = false)]
    server_address: Option<String>,

    /// docker hub server identity token
    #[clap(long, required = false)]
    identity_token: Option<String>,

    /// docker hub server registry token
    #[clap(long, required = false)]
    registry_token: Option<String>,
}

fn main() -> Result<()> {
    let rt = Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())
}

async fn run() -> Result<()> {
    let opts = Options::parse();

    simple_logger::SimpleLogger::new()
        .with_utc_timestamps()
        .with_level({
            match opts.debug {
                0 => log::LevelFilter::Info,
                1 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            }
        })
        .with_module_level("sqlx", log::Level::Error.to_level_filter())
        .init()?;

    let mut docker_image = opts.image_name.to_string();
    if !docker_image.contains(':') {
        docker_image.push_str(":latest");
    }

    let credentials = Some(DockerCredentials {
        username: opts.username,
        password: opts.password,
        auth: opts.auth,
        email: opts.email,
        serveraddress: opts.server_address,
        identitytoken: opts.identity_token,
        registrytoken: opts.registry_token,
    });

    let fl_name = docker_image.replace([':', '/'], "-") + ".fl";
    let meta = fungi::Writer::new(&fl_name, true).await?;
    let store = parse_router(&opts.store).await?;

    let container_name = Uuid::new_v4().to_string();
    let docker_tmp_dir =
        tempdir::TempDir::new(&container_name).expect("failed to create tmp directory");

    let mut docker_to_fl =
        docker2fl::DockerImageToFlist::new(meta, docker_image, credentials, docker_tmp_dir);
    let res = docker_to_fl.convert(store, None).await;

    // remove the file created with the writer if fl creation failed
    if res.is_err() {
        tokio::fs::remove_file(fl_name).await?;
        return res;
    }

    Ok(())
}
