use iron::typemap::Key;
use iron::middleware::{AfterMiddleware, BeforeMiddleware};
use iron::IronResult;
use iron::prelude::*;
use router::NoRoute;
use rand;
use std::sync::Mutex;

use error::*;
use web_server::chain;
use web_server::pages;
use std::sync::Arc;
use chrono::{DateTime, UTC};
use cache::Cache;
use config::Config;


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
    pub start_time: DateTime<UTC>,
    pub timings: Arc<Mutex<Vec<(i32, i64, DateTime<UTC>)>>>
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

struct ErrorHandler;

impl AfterMiddleware for ErrorHandler {
    fn catch(&self, request: &mut Request, err: IronError) -> IronResult<Response> {
        if err.error.is::<NoRoute>() {
            pages::not_found_page(request)
        } else {
            Ok(err.response)
        }
    }
}

pub fn run_server(cache: Cache, config: &Config) -> Result<()> {
    let request_timer = RequestTimer::new();
    let mut chain = chain::chain();
    chain.link_before(request_timer.clone());
    chain.link_before(cache);
    chain.link_before(ConfigArc::new(config.clone()));
    chain.link_after(request_timer.clone());
    chain.link_after(ErrorHandler);

    let port = 3000;
    let bind_addr = format!("localhost:{}", port);
    let _server_guard = Iron::new(chain).http(bind_addr.as_str())
        .chain_err(|| "Failed to start iron server")?;

    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("?");
    info!("Running kafka-web v{} on port {}.", version, port);

    Ok(())
}

