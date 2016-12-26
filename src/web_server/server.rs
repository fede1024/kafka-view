use iron::typemap::Key;
use iron::middleware::BeforeMiddleware;
use iron::prelude::*;

use error::*;
use cache::ReplicatedMap;
use web_server::chain;
use metadata::{ClusterId, Metadata};
use std::sync::Arc;


pub struct MetadataCache;

impl Key for MetadataCache { type Value = ReplicatedMap<ClusterId, Metadata>; }

impl BeforeMiddleware for ReplicatedMap<ClusterId, Metadata> {
    fn before(&self, request: &mut Request) -> IronResult<()> {
        request.extensions.insert::<MetadataCache>(self.alias());
        Ok(())
    }
}

pub fn run_server(metadata_cache: ReplicatedMap<ClusterId, Metadata>, debug: bool) -> Result<()> {
    // let metadata_cache_ref = MetadataCache { cache: metadata_cache };

    let mut chain = chain::chain();
    chain.link_before(metadata_cache);

    let port = 3000;
    let bind_addr = format!("localhost:{}", port);
    let _server_guard = Iron::new(chain).http(bind_addr.as_str())
        .chain_err(|| "Failed to start iron server")?;

    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("");
    info!("Running kafka-web v{} on port {}.", version, port);

    Ok(())
}

