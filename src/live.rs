use std::{
    fmt::Debug,
    iter::FromIterator,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use c2pa::{Manifest, Signer};
use reqwest::{blocking, Body, Client, IntoUrl};
use rocket::{
    data::ByteUnit,
    delete,
    http::Status,
    post,
    tokio::{
        fs::{self, File},
        io::{AsyncReadExt, AsyncWriteExt},
    },
    Data, State,
};
use url::Url;

use crate::signer::SignConfig;

const MAX_CHUNK_SIZE: usize = 65536; // 2^16

pub type Result<T> = core::result::Result<T, Status>;

/// removes all subdirectories contained in path
///
/// TODO: remove when everything is working
pub fn clear_media<P>(path: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let dir = std::fs::read_dir(path)?;

    for entry in dir {
        let Ok(entry) = entry else {
            log::warn!("unable to read {:?}", entry);
            continue;
        };

        if !entry.metadata()?.is_dir() {
            continue;
        }

        std::fs::remove_dir_all(entry.path())?;
    }

    Ok(())
}

/// receives the live stream, signs it and forward to cdn
#[post("/<name>/<uri..>", data = "<body>")]
pub async fn post_ingest(
    name: &str,
    uri: PathBuf,
    body: Data<'_>,
    state: &State<LiveSigner>,
) -> Result<()> {
    let local = state.local_path(name, &uri);
    let target = state.cdn_target(name, &uri)?;

    let payload = process_body(body, &local).await?;

    if is_mpd_path(&uri) {
        // (for now) forward the MPDs without altering them (maybe later adding the Merkle Tree to it)
        return state.post(target, Some(payload)).await;
    }

    if is_init_path(&uri) {
        // skip init, wait for first segment
        return Ok(());
    }

    state.sign_content(name, uri)
}

/// forwards delete requests to the cdn
#[delete("/<name>/<uri..>")]
pub async fn delete_ingest(name: &str, uri: PathBuf, state: &State<LiveSigner>) -> Result<()> {
    let target = state.cdn_target(name, uri)?;
    state.delete(target).await
}

/// reads the body, write it to disk and return it
async fn process_body<P>(body: Data<'_>, path: P) -> Result<Vec<u8>>
where
    P: AsRef<Path>,
{
    let mut file = create_file(path).await?;

    let mut body = body.open(ByteUnit::max_value());
    let mut payload = Vec::new();
    loop {
        let mut chunk = vec![0; MAX_CHUNK_SIZE];
        let read = body.read(&mut chunk).await.map_err(|err| {
            log::error!("failed to read body chunk: {err}");
            Status::InternalServerError
        })?;
        if read == 0 {
            // having read nothing means EOF
            break;
        }

        let chunk = &chunk[..read];
        payload.extend_from_slice(chunk);
        file.write(chunk).await.map_err(|err| {
            log::error!("failed to write chunk to file: {err}");
            Status::InternalServerError
        })?;
    }

    Ok(payload)
}

/// create a new file at path, will also create full path it it, if needed
async fn create_file<P>(path: P) -> Result<File>
where
    P: AsRef<Path>,
{
    create_path_to_file(&path).await?;

    File::create(&path).await.map_err(|err| {
        log::error!("failed to create file {:?}: {err}", path.as_ref());
        Status::InternalServerError
    })
}

/// create the path to a file, if needed
async fn create_path_to_file<P>(path: P) -> Result<()>
where
    P: AsRef<Path>,
{
    let dir = path.as_ref().parent().ok_or_else(|| {
        log::error!("path {:?} has no parent", path.as_ref());
        Status::InternalServerError
    })?;

    if dir.exists() {
        // directory already exists, no need to create it
        return Ok(());
    }

    fs::create_dir_all(dir).await.map_err(|err| {
        log::error!("failed to create path: {dir:?}: {err}");
        Status::InternalServerError
    })
}

/// checks if the file extension of path is "mpd"
fn is_mpd_path<P>(uri: P) -> bool
where
    P: AsRef<Path>,
{
    uri.as_ref().extension().is_some_and(|ext| ext == "mpd")
}

/// checks if the file contains "init"
fn is_init_path<P>(uri: P) -> bool
where
    P: AsRef<Path>,
{
    match uri.as_ref().to_str() {
        Some(s) => s.contains("init"),
        None => false,
    }
}

pub struct LiveSigner {
    pub media: PathBuf,
    pub target: Url,
    pub client: Client,
    pub sync_client: Arc<blocking::Client>,
    pub sign_config: Arc<SignConfig>,
    pub manifest: String,
}

impl LiveSigner {
    /// appends name and uri to local media directory
    pub fn local_path<P>(&self, name: &str, uri: P) -> PathBuf
    where
        P: AsRef<Path>,
    {
        self.media.join(name).join(uri)
    }

    /// appends name and uri to cdn target URL
    pub fn cdn_target<P>(&self, name: &str, uri: P) -> Result<Url>
    where
        P: AsRef<Path>,
    {
        let uri = uri.as_ref().as_os_str().to_str().ok_or_else(|| {
            log::error!("invalid path");
            Status::InternalServerError
        })?;
        self.target.join(&format!("{name}/{uri}")).map_err(|err| {
            log::error!("failed to join {name} to cdn target: {err}");
            Status::InternalServerError
        })
    }

    /// sends an async post request to url with optional body
    pub async fn post<U, T>(&self, url: U, body: Option<T>) -> Result<()>
    where
        U: IntoUrl + Clone + Debug,
        T: Into<Body> + Default,
    {
        self.client
            .post(url.clone())
            .body(body.unwrap_or_default())
            .send()
            .await
            .map_err(|err| {
                log::error!("failed to post sync to {url:?}: {err}");
                Status::InternalServerError
            })
            .map(|_| ())
    }

    /// sends an async delete request to url
    pub async fn delete<U>(&self, url: U) -> Result<()>
    where
        U: IntoUrl + Clone + Debug,
    {
        self.client
            .delete(url.clone())
            .send()
            .await
            .map_err(|err| {
                log::error!("failed to delete sync to {url:?}: {err}");
                Status::InternalServerError
            })
            .map(|_| ())
    }

    fn rep_id<P>(&self, uri: P) -> Result<String>
    where
        P: AsRef<Path>,
    {
        Ok(uri
            .as_ref()
            .components()
            .next()
            .ok_or_else(|| {
                log::error!("invalid uri");
                Status::InternalServerError
            })?
            .as_os_str()
            .to_str()
            .ok_or_else(|| {
                log::error!("invalid rep id path");
                Status::InternalServerError
            })?
            .to_string())
    }

    fn rep_dir<P>(&self, name: &str, uri: P) -> Result<PathBuf>
    where
        P: AsRef<Path>,
    {
        let rep_id = self.rep_id(uri)?;
        Ok(self.media.join(name).join(rep_id))
    }

    fn output<P>(&self, name: &str, uri: P) -> Result<PathBuf>
    where
        P: AsRef<Path>,
    {
        let rep_id = self.rep_id(uri)?;
        Ok(self.media.join(format!("signed_{name}")).join(rep_id))
    }

    fn sign_paths<P>(&self, name: &str, uri: P) -> Result<(PathBuf, Vec<PathBuf>)>
    where
        P: AsRef<Path>,
    {
        let mut init = None;
        let mut fragments = Vec::new();

        let dir = self.rep_dir(name, uri)?.read_dir().map_err(|err| {
            log::error!("failed to read {name} dir: {err}");
            Status::InternalServerError
        })?;
        for entry in dir {
            let entry = entry.map_err(|err| {
                log::error!("invalid dir entry: {err}");
                Status::InternalServerError
            })?;

            let path = entry.path();

            let path_str = path.to_str().ok_or_else(|| {
                log::error!("invalid path");
                Status::InternalServerError
            })?;
            if path_str.contains("init") {
                match init {
                    Some(_) => {
                        log::error!("multiple init files in one rep");
                        return Err(Status::InternalServerError);
                    }
                    None => {
                        init.replace(path);
                    }
                }
            } else {
                fragments.push(path);
            }
        }

        let init = init.ok_or_else(|| {
            log::error!("couldn't find init file");
            Status::NotFound
        })?;
        Ok((init, fragments))
    }

    fn forward_paths<P>(&self, name: &str, uri: P) -> Result<Vec<(Url, PathBuf)>>
    where
        P: AsRef<Path>,
    {
        let mut paths = Vec::new();
        let mut fragments = Vec::new();

        let dir = self.rep_dir(name, uri)?.read_dir().map_err(|err| {
            log::error!("failed to read {name} dir: {err}");
            Status::InternalServerError
        })?;
        for entry in dir {
            let entry = entry.map_err(|err| {
                log::error!("invalid dir entry: {err}");
                Status::InternalServerError
            })?;

            let path = entry.path();

            let target = self.path_to_target(&path)?;
            let path = self.path_to_signed_path(name, path)?;

            // push init to paths to have it as the first element
            match is_init_path(&path) {
                true => paths.push((target, path)),
                false => fragments.push((target, path)),
            }
        }

        // sort by inverse path order to have the newest paths first
        fragments.sort_by(|(_, a), (_, b)| b.cmp(a));
        fragments.iter().for_each(|p| paths.push(p.clone()));

        Ok(paths)
    }

    fn path_to_target<P>(&self, path: P) -> Result<Url>
    where
        P: AsRef<Path>,
    {
        let t = path
            .as_ref()
            .strip_prefix(&self.media)
            .map_err(|err| {
                log::error!("failed to strip media path for url: {err}");
                Status::InternalServerError
            })?
            .as_os_str()
            .to_str()
            .ok_or_else(|| {
                log::error!("invalid path");
                Status::InternalServerError
            })?;

        self.target.join(t).map_err(|err| {
            log::error!("failed to join url: {err}");
            Status::InternalServerError
        })
    }

    fn path_to_signed_path<P>(&self, name: &str, path: P) -> Result<PathBuf>
    where
        P: AsRef<Path>,
    {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| {
                log::error!("invalid path");
                Status::InternalServerError
            })?
            .split("/")
            .map(|c| {
                if c == name {
                    format!("signed_{name}")
                } else {
                    c.to_string()
                }
            })
            .collect::<Vec<String>>();
        Ok(PathBuf::from_iter(path))
    }

    /// signs the new fragment
    pub fn sign_content<P>(&self, name: &str, uri: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let thread_name = format!("{name} - {:?}", uri.as_ref());
        let sign_config = self.sign_config.clone();
        let manifest_json = self.manifest.clone();
        let (init, fragments) = self.sign_paths(name, &uri)?;
        let output = self.output(name, &uri)?;
        let forward = self.forward_paths(name, uri)?;
        let client = self.sync_client.clone();

        thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || -> Result<()> {
                let mut manifest = Manifest::from_json(&manifest_json).map_err(|err| {
                    log::error!("failed to parse Manifest from json: {err}");
                    Status::InternalServerError
                })?;
                let signer = signer(sign_config)?;

                manifest
                    .embed_to_bmff_fragmented(init, &fragments, output, signer.as_ref())
                    .map_err(|err| {
                        log::error!("failed to sign stream: {err}");
                        Status::InternalServerError
                    })?;

                for (url, path) in forward {
                    let payload = std::fs::read(&path).map_err(|err| {
                        log::error!("failed to read signed file: {path:?}: {err}");
                        Status::InternalServerError
                    })?;

                    client.post(url).body(payload).send().map_err(|err| {
                        log::error!("failed to forward signed fragment to cdn: {err}");
                        Status::InternalServerError
                    })?;
                }

                Ok(())
            })
            .map_err(|err| {
                log::error!("running thread {thread_name}: {err}");
                Status::InternalServerError
            })?;

        Ok(())
    }
}

/// extracts the C2PA Signer from a sign config
fn signer(sign_config: Arc<SignConfig>) -> Result<Box<dyn Signer>> {
    sign_config.signer().map_err(|err| {
        log::error!("failed to parse Signer from Manifest: {}", err);
        Status::InternalServerError
    })
}
