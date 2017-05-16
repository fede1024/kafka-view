use iron::typemap::Key;
use iron::middleware::{AfterMiddleware, BeforeMiddleware};
use iron::IronResult;
use iron::prelude::*;
use router::NoRoute;
use rand;
use std::sync::Mutex;
use rocket;

use error::*;
use web_server::pages;
use web_server::api;
use std::sync::Arc;
use chrono::{DateTime, UTC};
use cache::Cache;
use config::Config;
use metadata::ClusterId;

use rocket::response::{self, Redirect, Responder, NamedFile};
use rocket::request::FromParam;

use std::path::{Path, PathBuf};
use std::time;
use std;
use std::cell::RefCell;



pub struct CacheType;

impl Key for CacheType { type Value = Cache; }

impl BeforeMiddleware for Cache {
   fn before(&self, request: &mut Request) -> IronResult<()> {
       request.extensions.insert::<CacheType>(self.alias());
       Ok(())
   }
}

#[derive(Clone)]
pub struct ConfigArc {
    pub config: Arc<Config>
}

impl ConfigArc {
    fn new(config: Config) -> ConfigArc {
        ConfigArc { config: Arc::new(config) }
    }
}

impl Key for ConfigArc { type Value = ConfigArc; }

impl BeforeMiddleware for ConfigArc {
    fn before(&self, request: &mut Request) -> IronResult<()> {
        request.extensions.insert::<ConfigArc>(self.clone());
        Ok(())
    }
}

#[derive(Clone)]
pub struct RequestTimer {
     pub request_id: i32,
     pub start_time: DateTime < UTC >,
     pub timings: Arc< Mutex < Vec < (i32, i64, DateTime < UTC > ) > > >
}

impl RequestTimer {
    fn new() -> RequestTimer {
        RequestTimer {
            request_id: 0,           // Value not used
            start_time: UTC::now(),  // Value not used
            timings: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn update_timing(&self) {
        let now = UTC::now();
        let elapsed_micros = now.signed_duration_since(self.start_time).num_microseconds().unwrap();
        let mut timings = self.timings.lock().expect("Poison error");
        timings.retain(|&(_, _, request_time)| now.signed_duration_since(request_time).num_seconds() < 20);
        if timings.len() < 1000 {
            timings.push((self.request_id, elapsed_micros, now));
        }
    }
}

impl Key for RequestTimer { type Value = RequestTimer; }

impl BeforeMiddleware for RequestTimer {
    fn before(&self, request: &mut Request) -> IronResult<()> {
        // TODO: improve
        let path_len = request.url.path().last().unwrap_or(&"").len();
        if path_len == 0 {
            let mut request_timer = self.clone();
            request_timer.request_id = rand::random::<i32>();
            request_timer.start_time = UTC::now();
            request.extensions.insert::<RequestTimer>(request_timer);
        }
        Ok(())
    }
}

impl AfterMiddleware for RequestTimer {
    fn after(&self, request: &mut Request, response: Response) -> IronResult<Response> {
        request.extensions.get::<RequestTimer>()
            .map(|request_timer| request_timer.update_timing());
        Ok(response)
    }

    fn catch(&self, request: &mut Request, err: IronError) -> IronResult<Response> {
        request.extensions.get::<RequestTimer>()
            .map(|request_timer| request_timer.update_timing());
        Err(err)
    }
}

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
            pages::clusters::clusters_page,
            pages::cluster::cluster_page,
            pages::topic::topic_page,
            pages::omnisearch::consumer_search,
            pages::omnisearch::consumer_search_p,
            pages::internals::caches_page,
            pages::group::group_page,
            api::brokers,
            api::cluster_groups,
            api::cluster_topics,
            api::group_members,
            api::group_offsets,
            api::topic_groups,
            api::cache_brokers,
            api::cache_metrics,
            api::topic_topology
        ])
        .launch();

    Ok(())
}

