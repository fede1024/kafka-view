#![feature(alloc_system, custom_derive, plugin, conservative_impl_trait)]
#![plugin(rocket_codegen)]
#![plugin(maud_macros)]

extern crate rocket;
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
extern crate maud;
extern crate mount;
extern crate rand;
extern crate rdkafka;
extern crate regex;
extern crate serde;
extern crate serde_cbor;
extern crate serde_yaml;
extern crate scheduled_executor;

#[macro_use] mod utils;
mod cache;
mod config;
mod error;
mod live_consumer;
mod metadata;
mod metrics;
mod web_server;
mod offsets;

use clap::{App, Arg, ArgMatches};
use scheduled_executor::{thread_pool, Executor, TaskGroup};
use std::time::Duration;

use cache::{Cache, ReplicaReader, ReplicaWriter};
use error::*;
use metrics::MetricsFetchTaskGroup;
use metadata::MetadataFetchTaskGroup;
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
    let start_time = chrono::UTC::now();
    replica_reader.load_state(cache.alias())
        .chain_err(|| format!("State load failed (brokers: {}, topic: {})", replicator_bootstrap_servers, topic_name))?;
    let elapsed_sec = chrono::UTC::now().signed_duration_since(start_time).num_milliseconds() as f32 / 1000f32;
    info!("Processed {} messages in {:.3} seconds ({:.0} msg/s).",
        replica_reader.processed_messages(), elapsed_sec, replica_reader.processed_messages() as f32 / elapsed_sec);

    let executor = Executor::new("executor")
        .chain_err(|| "Failed to start main executor")?;
    let pool = thread_pool(4, "data-fetch-");

    // Metadata fetch
    let metadata_task_group = MetadataFetchTaskGroup::new(&cache, &config);
    metadata_task_group.schedule(Duration::from_secs(config.metadata_refresh), &executor, Some(pool.clone()));

    // Metrics fetch
    let metrics_task_group = MetricsFetchTaskGroup::new(&cache, &config);
    metrics_task_group.schedule(Duration::from_secs(config.metrics_refresh), &executor, Some(pool.clone()));

    // Consumer offsets
    for (cluster_id, cluster_config) in &config.clusters {
        if let Err(e) = run_offset_consumer(cluster_id, cluster_config, &config, &cache) {
            format_error_chain!(e);
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
