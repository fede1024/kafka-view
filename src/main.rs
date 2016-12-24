#![feature(alloc_system, proc_macro, plugin)]
#![plugin(maud_macros)]
extern crate alloc_system;

#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate clap;
extern crate env_logger;
extern crate handlebars_iron as hbi;
extern crate iron;
extern crate chrono;
extern crate futures;
extern crate maud;
extern crate mount;
extern crate persistent;
extern crate rdkafka;
extern crate staticfile;
extern crate urlencoded;
extern crate serde;


mod cache;
mod config;
mod error;
mod metadata;
mod scheduler;
mod utils;
mod web_server;

use error::*;
use metadata::MetadataFetcher;

use clap::{App, Arg, ArgMatches};

use std::time;
use std::thread;

use cache::{Cache, Replicator};

extern crate serde_json;

fn run_kafka_web(config_path: &str) -> Result<()> {
    let config = config::read_config(config_path)
        .chain_err(|| format!("Unable to load configuration from '{}'", config_path))?;

    let replicator = Replicator::new("localhost:9092", "replicator_topic")
        .chain_err(|| format!("Replicator creation failed (brokers: {}, topic: {})", "localhost:9092", "replicator_topic"))?;
    let metadata_cache = replicator.create_cache::<Cache<_, _>>("metadata");
    let mut metadata_fetcher = MetadataFetcher::new(metadata_cache.clone(), time::Duration::from_secs(15));
    for (cluster_name, cluster_config) in config.clusters() {
        metadata_fetcher.add_cluster(cluster_name, &cluster_config.broker_string())
            .chain_err(|| format!("Failed to add cluster {}", cluster_name))?;
        info!("Added cluster {}", cluster_name);
    }

    web_server::server::run_server(metadata_cache, true)
        .chain_err(|| "Server initialization failed")?;

    loop {
        thread::sleep_ms(100000);
    };
}

fn setup_args<'a>() -> ArgMatches<'a> {
    App::new("kafka web interface")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Kafka web interface")
        .arg(Arg::with_name("conf")
            .short("c")
            .long("conf")
            .help("Configuration file")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("log-conf")
            .long("log-conf")
            .help("Configure the logging format (example: 'rdkafka=trace')")
            .takes_value(true))
        .get_matches()
}

fn main() {
    let matches = setup_args();

    utils::setup_logger(true, matches.value_of("log-conf"), "%F %T%z");

    let config_path = matches.value_of("conf").unwrap();

    if let Err(ref e) = run_kafka_web(config_path) {
        println!("error: {}", e);

        for e in e.iter().skip(1) {
            println!("caused by: {}", e);
        }

        // The backtrace is not always generated. Try to run this example
        // with `RUST_BACKTRACE=1`.
        if let Some(backtrace) = e.backtrace() {
            println!("backtrace: {:?}", backtrace);
        }

        std::process::exit(1);
    }
}
