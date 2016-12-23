#![feature(plugin)]
extern crate env_logger;
//extern crate error_chain;
extern crate handlebars as hbs;
extern crate handlebars_iron as hbi;
extern crate iron;
extern crate mount;
extern crate router;
extern crate staticfile;
extern crate urlencoded;
extern crate persistent;

use self::iron::prelude::Iron;
use self::iron::typemap::Key;
use self::hbs::Handlebars;
use self::hbi::{HandlebarsEngine};
use self::persistent::State;

use error::*;
use cache::{Cache, ReplicatedCache};
use web_server::chain;
use metadata::{ClusterId, Metadata};
use std::sync::Arc;
use std::error::Error;
use std::cell::Cell;


pub struct MetadataCache;

impl Key for MetadataCache { type Value = Cache<ClusterId, Metadata>; }

fn load_templates(path: &str, ext: &str) -> Result<HandlebarsEngine> {
    let mut hbs = Handlebars::new();
    let mut hbse = HandlebarsEngine::from(hbs);

    // TODO: Investigate serving the templates out of the binary using include_string!
    hbse.add(Box::new(hbi::DirectorySource::new(path, ext)));
    // hbse.reload().chain_err(|| "Failed to load template")?;
    if let Err(e) = hbse.reload() {
        bail!("Error: {:?}", e);
    }

    Ok(hbse)
}

pub fn run_server(metadata_cache: Cache<ClusterId, Metadata>, debug: bool) -> Result<()> {
    let templates_ext = ".hbs";
    let templates_path = "./resources/web_server/templates";

    let hbse = load_templates(templates_path, templates_ext)
        .chain_err(|| "Failed to initialize templates")?;
    let hbse_ref = Arc::new(hbse);

    if debug {
        warn!("WARNING: DEBUG ASSERTIONS ENABLED. TEMPLATES ARE WATCHED.");
        use self::hbi::Watchable;
        hbse_ref.watch(templates_path);
    }

    let mut chain = chain::chain();
    chain.link_after(hbse_ref);
    chain.link(State::<MetadataCache>::both(metadata_cache));

    let port = 3000;
    let bind_addr = format!("localhost:{}", port);
    let _server_guard = Iron::new(chain).http(bind_addr.as_str())
        .chain_err(|| "Failed to start iron server")?;

    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("");
    info!("Running WLB v{} on port {}.", version, port);

    Ok(())
}

