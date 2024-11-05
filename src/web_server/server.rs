use rocket;
use rocket::figment::providers::Env;
use rocket::form::FromFormField;
use rocket::fs::NamedFile;
use rocket::http::RawStr;
use rocket::request::{FromParam, Request};
use rocket::response::{self, Redirect, Responder};
use scheduled_executor::ThreadPoolExecutor;

use crate::cache::Cache;
use crate::config::Config;
use crate::error::*;
use crate::live_consumer::{self, LiveConsumerStore};
use crate::metadata::ClusterId;
use crate::utils::{GZip, RequestLogger};
use crate::web_server::api;
use crate::web_server::pages;

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::{self, env};

#[get("/")]
fn index() -> Redirect {
    Redirect::to("/clusters")
}

// Make ClusterId a valid parameter
impl<'a> FromParam<'a> for ClusterId {
    type Error = ();

    fn from_param(param: &'a str) -> std::result::Result<Self, Self::Error> {
        Ok(param.into())
    }
}

#[get("/public/<file..>")]
async fn files(file: PathBuf) -> Option<CachedFile> {
    NamedFile::open(Path::new("resources/web_server/public/").join(file))
        .await
        .map(CachedFile::from)
        .ok()
}

#[get("/public/<file..>?<version>")]
async fn files_v<'a>(file: PathBuf, version: &'a str) -> Option<CachedFile> {
    let _ = version; // just ignore version
    NamedFile::open(Path::new("resources/web_server/public/").join(file))
        .await
        .map(CachedFile::from)
        .ok()
}

pub struct CachedFile {
    ttl: usize,
    file: NamedFile,
}

impl CachedFile {
    pub fn from(file: NamedFile) -> CachedFile {
        CachedFile::with_ttl(1800, file)
    }

    pub fn with_ttl(ttl: usize, file: NamedFile) -> CachedFile {
        CachedFile { ttl, file }
    }
}

impl<'r, 'o: 'r> Responder<'r, 'r> for CachedFile {
    fn respond_to(self, request: &Request) -> response::Result<'r> {
        let inner_response = self.file.respond_to(request).unwrap(); // fixme
        response::Response::build_from(inner_response)
            .raw_header(
                "Cache-Control",
                format!("max-age={}, must-revalidate", self.ttl),
            )
            .ok()
    }
}

pub async fn run_server(executor: &ThreadPoolExecutor, cache: Cache, config: &Config) -> Result<()> {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("?");
    info!(
        "Starting kafka-view v{}, listening on {}:{}.",
        version, config.listen_host, config.listen_port
    );

    let mut rocket_config = rocket::config::Config::from(
        rocket::config::Config::figment().merge(Env::prefixed("ROCKET_ENV")),
    );
    rocket_config.address = (config.listen_host.to_owned())
        .parse::<IpAddr>()
        .unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
        .into();
    rocket_config.port = config.listen_port;
    rocket_config.workers = 4;

    let _ = rocket::custom(rocket_config)
        .attach(GZip)
        .attach(RequestLogger)
        .manage(cache)
        .manage(config.clone())
        .manage(LiveConsumerStore::new(executor.clone()))
        .mount(
            "/",
            routes![
                index,
                files,
                files_v,
                pages::cluster::cluster_page,
                pages::cluster::broker_page,
                pages::clusters::clusters_page,
                pages::group::group_page,
                pages::internals::caches_page,
                pages::internals::live_consumers_page,
                pages::omnisearch::consumer_search,
                pages::omnisearch::consumer_search_p,
                pages::omnisearch::omnisearch,
                pages::omnisearch::omnisearch_p,
                pages::omnisearch::topic_search,
                pages::omnisearch::topic_search_p,
                pages::topic::topic_page,
                api::brokers,
                api::cache_brokers,
                api::cache_metrics,
                api::cache_offsets,
                api::cluster_reassignment,
                api::live_consumers,
                api::cluster_groups,
                api::cluster_topics,
                api::consumer_search,
                api::group_members,
                api::group_offsets,
                api::topic_groups,
                api::topic_search,
                api::topic_topology,
                live_consumer::topic_tailer_api,
            ],
        )
        .launch().await.chain_err(|| "Failed to launch rocket");

    Ok(())
}
