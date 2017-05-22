use rocket::request::FromParam;
use rocket::response::{self, Redirect, Responder, NamedFile};
use rocket;

use error::*;
use web_server::pages;
use web_server::api;
use cache::Cache;
use config::Config;
use metadata::ClusterId;

use std::path::{Path, PathBuf};
use std;


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
fn files(file: PathBuf) -> Option<CachedFile> {
    NamedFile::open(Path::new("resources/web_server/public/").join(file))
        .map(CachedFile::from)
        .ok()
}

#[get("/public/<file..>?<_version>")]
fn files_v(file: PathBuf, _version: &str) -> Option<CachedFile> {
    NamedFile::open(Path::new("resources/web_server/public/").join(file))
        .map(CachedFile::from)
        .ok()
}

pub struct CachedFile {
    ttl: usize,
    file: NamedFile,
}

impl CachedFile {
    pub fn from(file: NamedFile) -> CachedFile {
        CachedFile::with_ttl(7200, file)
    }

    pub fn with_ttl(ttl: usize, file: NamedFile) -> CachedFile {
        CachedFile { ttl, file }
    }
}

impl<'a> Responder<'a> for CachedFile {
    fn respond(self) -> response::Result<'a> {
        let inner_response = self.file.respond().unwrap(); // fixme
        response::Response::build_from(inner_response)
            .raw_header("Cache-Control", format!("max-age={}, must-revalidate", self.ttl))
            //.raw_header("Content-Encoding", "gzip")
            .ok()
    }
}

pub fn run_server(cache: Cache, config: &Config) -> Result<()> {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("?");
    info!("Starting kafka-view v{}, listening on {}:{}.", version, config.listen_host, config.listen_port);

    let rocket_config = rocket::config::Config::build(rocket::config::Environment::Development)
        .address(config.listen_host.to_owned())
        .port(config.listen_port)
        .workers(32)
        .log_level(rocket::logger::LoggingLevel::Critical)
        .finalize()
        .chain_err(|| "Invalid rocket configuration")?;

    rocket::custom(rocket_config, false)
        .manage(cache)
        .manage(config.clone())
        .mount("/", routes![
            index,
            files,
            files_v,
            pages::cluster::cluster_page,
            pages::clusters::clusters_page,
            pages::group::group_page,
            pages::internals::caches_page,
            pages::omnisearch::consumer_search,
            pages::omnisearch::consumer_search_p,
            pages::omnisearch::omni_search,
            pages::omnisearch::omni_search_p,
            pages::omnisearch::topic_search,
            pages::omnisearch::topic_search_p,
            pages::topic::topic_page,
            api::brokers,
            api::cache_brokers,
            api::cache_metrics,
            api::cluster_groups,
            api::cluster_topics,
            api::consumer_search,
            api::group_members,
            api::group_offsets,
            api::topic_groups,
            api::topic_search,
            api::topic_topology,
        ])
        .launch();

    Ok(())
}

