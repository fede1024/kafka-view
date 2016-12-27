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

use rdkafka::message::Message;

use cache::{ReplicatedMap, ReplicaReader, ReplicaWriter};
use error::*;
use metadata::{Metadata, MetadataFetcher};
use utils::format_error_chain;


fn state_update(name: String, key_str: String, msg: Message, metadata: &ReplicatedMap<String, Metadata>) -> Result<()> {
    match name.as_ref() {
        "metadata" => {
            let key = serde_json::from_str::<String>(&key_str)
                .chain_err(|| "Failed to parse key")?;
            match msg.payload() {
                Some(bytes) => {
                    let payload = serde_json::from_slice::<Metadata>(bytes)
                        .chain_err(|| "Failed to parse payload")?;
                    debug!("Sync metadata cache, key: {}", key);
                    metadata.sync_value_update(key, payload)
                        .chain_err(|| format!("Failed to sync value in cache {}", name))?;
                },
                None => bail!("Delete not implemented!"),
            };
        },
        _ => {
            bail!("Unknown cache name: {}", name);
        },
    };
    Ok(())
}

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

    let metadata_cache_alias = metadata_cache.alias();
    replica_reader.start(move |name, key_str, msg| {
        if let Err(ref e) = state_update(name, key_str, msg, &metadata_cache_alias) {
            format_error_chain(e);
        }
    })
        .chain_err(|| format!("Replica reader start failed (brokers: {}, topic: {})", brokers, topic_name))?;

    let mut metadata_fetcher = MetadataFetcher::new(metadata_cache.alias(), time::Duration::from_secs(60));
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
        format_error_chain(e);
        std::process::exit(1);
    }
}
