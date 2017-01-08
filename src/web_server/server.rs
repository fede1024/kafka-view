use iron::typemap::Key;
use iron::middleware::{AfterMiddleware, BeforeMiddleware};
use iron::{IronResult, status};
use maud::PreEscaped;
use iron::headers;
use iron::prelude::*;
use router::NoRoute;

use error::*;
use web_server::view::layout;
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

pub struct RequestTimer;
impl Key for RequestTimer { type Value = DateTime<UTC>; }

impl BeforeMiddleware for RequestTimer {
    fn before(&self, request: &mut Request) -> IronResult<()> {
        request.extensions.insert::<RequestTimer>(UTC::now());
        Ok(())
    }
}

impl AfterMiddleware for RequestTimer {
    fn after(&self, request: &mut Request, mut response: Response) -> IronResult<Response> {
        let time = request.extensions.get::<RequestTimer>().unwrap();
        let millis = (UTC::now() - *time).num_milliseconds().to_string();
        let mut cookie = headers::CookiePair::new("request_time".to_owned(), millis.to_string());
        cookie.max_age = Some(20);
        response.headers.set(headers::SetCookie(vec![cookie]));
        Ok(response)
    }
}

struct ErrorHandler;

impl AfterMiddleware for ErrorHandler {
    fn catch(&self, request: &mut Request, err: IronError) -> IronResult<Response> {
        if err.error.is::<NoRoute>() {
            pages::not_found(request)
        } else {
            Ok(err.response)
        }
    }
}

pub fn run_server(cache: Cache, config: &Config) -> Result<()> {
    let mut chain = chain::chain();
    chain.link_before(RequestTimer);
    chain.link_before(cache);
    chain.link_before(ConfigArc::new(config.clone()));
    chain.link_after(RequestTimer);
    chain.link_after(ErrorHandler);

    let port = 3000;
    let bind_addr = format!("localhost:{}", port);
    let _server_guard = Iron::new(chain).http(bind_addr.as_str())
        .chain_err(|| "Failed to start iron server")?;

    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("?");
    info!("Running kafka-web v{} on port {}.", version, port);

    Ok(())
}

