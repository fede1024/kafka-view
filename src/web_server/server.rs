use iron::typemap::Key;
use iron::middleware::{AfterMiddleware, BeforeMiddleware};
use iron::headers;
use iron::prelude::*;

use error::*;
use cache::ReplicatedMap;
use web_server::chain;
use metadata::{ClusterId, Metadata};
use std::sync::Arc;
// use std::atomic::AtomicLong;
use chrono::{DateTime, UTC};


pub struct MetadataCache;

impl Key for MetadataCache { type Value = ReplicatedMap<ClusterId, Arc<Metadata>>; }

impl BeforeMiddleware for ReplicatedMap<ClusterId, Arc<Metadata>> {
    fn before(&self, request: &mut Request) -> IronResult<()> {
        request.extensions.insert::<MetadataCache>(self.alias());
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
        let pair = headers::CookiePair::new("request_time".to_owned(), millis.to_string());
        response.headers.set(headers::SetCookie(vec![pair]));
        Ok(response)
    }
}

pub fn run_server(metadata_cache: ReplicatedMap<ClusterId, Arc<Metadata>>, debug: bool) -> Result<()> {
    // let metadata_cache_ref = MetadataCache { cache: metadata_cache };

    let mut chain = chain::chain();
    chain.link_before(RequestTimer);
    chain.link_before(metadata_cache);
    chain.link_after(RequestTimer);

    let port = 3000;
    let bind_addr = format!("localhost:{}", port);
    let _server_guard = Iron::new(chain).http(bind_addr.as_str())
        .chain_err(|| "Failed to start iron server")?;

    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("?");
    info!("Running kafka-web v{} on port {}.", version, port);

    Ok(())
}

