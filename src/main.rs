#![feature(alloc_system, plugin, conservative_impl_trait)]
#![plugin(maud_macros)]
extern crate alloc_system;

#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate lazy_static;
extern crate byteorder;
extern crate chrono;
extern crate clap;
extern crate curl;
extern crate env_logger;
extern crate futures;
extern crate futures_cpupool;
extern crate iron;
extern crate iron_compress;
extern crate itertools;
extern crate maud;
extern crate mount;
extern crate rand;
extern crate rdkafka;
extern crate regex;
extern crate router;
extern crate serde;
extern crate serde_cbor;
extern crate staticfile;
extern crate urlencoded;

#[macro_use] mod utils;
mod cache;
mod config;
mod error;
mod metadata;
mod metrics;
mod scheduler;
mod web_server;
mod offsets;

use clap::{App, Arg, ArgMatches};

use std::time;
use time::Duration;

use cache::{Cache, ReplicaReader, ReplicaWriter};
use error::*;
use metrics::MetricsFetcher;
use metadata::MetadataFetcher;

use offsets::run_offset_consumer;

fn run_kafka_web(config_path: &str) -> Result<()> {
    let config = config::read_config(config_path)
        .chain_err(|| format!("Unable to load configuration from '{}'", config_path))?;

    let replicator_bootstrap_servers = match config.cluster(&config.caching.cluster) {
        Some(cluster) => cluster.bootstrap_servers(),
        None => bail!("Can't find cache cluster {}", config.caching.cluster),
    };
    let topic_name = &config.caching.topic;
    let replica_writer = ReplicaWriter::new(&replicator_bootstrap_servers, topic_name)
        .chain_err(|| format!("Replica writer creation failed (brokers: {}, topic: {})", replicator_bootstrap_servers, topic_name))?;
    let mut replica_reader = ReplicaReader::new(&replicator_bootstrap_servers, topic_name)
        .chain_err(|| format!("Replica reader creation failed (brokers: {}, topic: {})", replicator_bootstrap_servers, topic_name))?;

    let cache = Cache::new(replica_writer);

    // Load all the state from Kafka
    replica_reader.load_state(cache.alias())
        .chain_err(|| format!("State load failed (brokers: {}, topic: {})", replicator_bootstrap_servers, topic_name))?;

    // Metadata fetch
    let mut metadata_fetcher = MetadataFetcher::new(cache.brokers.alias(), cache.topics.alias(),
            cache.groups.alias(), Duration::from_secs(config.metadata_refresh));
    for (cluster_id, cluster_config) in &config.clusters {
        metadata_fetcher.add_cluster(cluster_id, &cluster_config)
            .chain_err(|| format!("Failed to add cluster {}", cluster_id))?;
        run_offset_consumer(&cluster_id, &cluster_config, &config, cache.offsets.alias());
        info!("Added cluster {}", cluster_id);
    }

    let mut metrics_fetcher = MetricsFetcher::new(cache.metrics.alias(),
        Duration::from_secs(config.metrics_refresh));
    for cluster_id in &cache.brokers.keys() {
        // TODO there is a race condition here, a broker could be removed
        for broker in cache.brokers.get(cluster_id).unwrap().iter() {
            metrics_fetcher.add_broker(cluster_id, broker.id, &broker.hostname);
        }
    }

    web_server::server::run_server(cache.alias(), &config)
        .chain_err(|| "Server initialization failed")?;

    Ok(())
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

    info!("Kafka-view is starting up!");
    if let Err(e) = run_kafka_web(config_path) {
        format_error_chain!(e);
        std::process::exit(1);
    }
}
