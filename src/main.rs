#![feature(alloc_system, proc_macro, plugin)]
#![plugin(maud_macros)]
extern crate alloc_system;

#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate clap;
extern crate env_logger;
extern crate iron;
extern crate chrono;
extern crate futures;
extern crate maud;
extern crate mount;
extern crate rdkafka;
extern crate staticfile;
extern crate urlencoded;
extern crate serde;
extern crate serde_json;


mod cache;
mod config;
mod error;
mod metadata;
mod scheduler;
mod utils;
mod web_server;

use clap::{App, Arg, ArgMatches};

use std::time;
use std::thread;
use std::sync::Arc;

use cache::{ReplicatedMap, ReplicaReader, ReplicaWriter};
use error::*;
use metadata::{Metadata, MetadataFetcher};


fn run_kafka_web(config_path: &str) -> Result<()> {
    let config = config::read_config(config_path)
        .chain_err(|| format!("Unable to load configuration from '{}'", config_path))?;
    let brokers = "localhost:9092";
    let topic_name = "replicator_topic";

    let replica_writer = ReplicaWriter::new(brokers, topic_name)
        .chain_err(|| format!("Replica writer creation failed (brokers: {}, topic: {})", brokers, topic_name))?;
    let metadata_cache: ReplicatedMap<String, Metadata> = ReplicatedMap::new("metadata", replica_writer.alias());

    let mut replica_reader = ReplicaReader::new(brokers, "replicator_topic")
        .chain_err(|| format!("Replica reader creation failed (brokers: {}, topic: {})", brokers, topic_name))?;
    replica_reader.start()
        .chain_err(|| format!("Replica reader start failed (brokers: {}, topic: {})", brokers, topic_name))?;

    let mut metadata_fetcher = MetadataFetcher::new(metadata_cache.alias(), time::Duration::from_secs(15));
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
