use rocket;
use rocket::http::hyper::header::CacheControl;
use rocket::http::hyper::header::CacheDirective;
use rocket::http::{ContentType, RawStr, Status};
use rocket::request::FromParam;
use rocket::response::{self, Redirect};
use scheduled_executor::ThreadPoolExecutor;

use cache::Cache;
use config::Config;
use error::*;
use live_consumer::{self, LiveConsumerStore};
use metadata::ClusterId;
use std::ffi::OsStr;
use std::io::Cursor;
use utils::{GZip, RequestLogger};
use web_server::api;
use web_server::pages;

use std;
use std::path::PathBuf;

#[derive(RustEmbed)]
#[folder = "resources/web_server/public/"]
struct Asset;

#[get("/")]
fn index() -> Redirect {
    Redirect::to("/clusters")
}

// Make ClusterId a valid parameter
impl<'a> FromParam<'a> for ClusterId {
    type Error = ();

    fn from_param(param: &'a RawStr) -> std::result::Result<Self, Self::Error> {
        Ok(param.as_str().into())
    }
}

#[get("/public/<file..>")]
fn files<'r>(file: PathBuf) -> response::Result<'r> {
    let filename = file.display().to_string();
    Asset::get(&filename).map_or_else(
        || Err(Status::NotFound),
        |d| {
            let ext = file
                .as_path()
                .extension()
                .and_then(OsStr::to_str)
                .ok_or(Status::new(400, "Could not get file extension"))?;
            let content_type = ContentType::from_extension(ext)
                .ok_or(Status::new(400, "Could not get file content type"))?;
            response::Response::build()
                .header(content_type)
                .header(CacheControl(vec![
                    CacheDirective::MustRevalidate,
                    CacheDirective::MaxAge(1800),
                ]))
                .sized_body(Cursor::new(d))
                .ok()
        },
    )
}

#[get("/public/<file..>?<version>")]
fn files_v<'r>(file: PathBuf, version: &RawStr) -> response::Result<'r> {
    let _ = version; // just ignore version
    let filename = file.display().to_string();
    Asset::get(&filename).map_or_else(
        || Err(Status::NotFound),
        |d| {
            let ext = file
                .as_path()
                .extension()
                .and_then(OsStr::to_str)
                .ok_or(Status::new(400, "Could not get file extension"))?;
            let content_type = ContentType::from_extension(ext)
                .ok_or(Status::new(400, "Could not get file content type"))?;
            response::Response::build()
                .header(content_type)
                .header(CacheControl(vec![
                    CacheDirective::MustRevalidate,
                    CacheDirective::MaxAge(1800),
                ]))
                .sized_body(Cursor::new(d))
                .ok()
        },
    )
}

pub fn run_server(executor: &ThreadPoolExecutor, cache: Cache, config: &Config) -> Result<()> {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("?");
    info!(
        "Starting kafka-view v{}, listening on {}:{}.",
        version, config.listen_host, config.listen_port
    );

    let rocket_env = rocket::config::Environment::active()
        .chain_err(|| "Invalid ROCKET_ENV environment variable")?;
    let rocket_config = rocket::config::Config::build(rocket_env)
        .address(config.listen_host.to_owned())
        .port(config.listen_port)
        .workers(4)
        .finalize()
        .chain_err(|| "Invalid rocket configuration")?;

    rocket::custom(rocket_config)
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
        .launch();

    Ok(())
}
