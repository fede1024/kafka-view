

#[macro_use]
extern crate error_chain;
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate lazy_static;
extern crate brotli;
extern crate byteorder;
extern crate chrono;
extern crate clap;
extern crate env_logger;
extern crate flate2;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
extern crate maud;
extern crate rand;
extern crate rdkafka;
extern crate regex;
#[macro_use]
extern crate rocket;
extern crate scheduled_executor;
extern crate serde;
extern crate serde_yaml;
extern crate zookeeper;

#[macro_use]
mod utils;
mod cache;
mod config;
mod error;
mod live_consumer;
mod metadata;
mod metrics;
mod offsets;
mod web_server;
mod zk;

use clap::{App, Arg, ArgMatches};
use rocket::tokio;
use scheduled_executor::{TaskGroupScheduler, ThreadPoolExecutor};
use std::time::Duration;

use crate::cache::{Cache, ReplicaReader, ReplicaWriter};
use crate::error::*;
use crate::metadata::MetadataFetchTaskGroup;
use crate::metrics::MetricsFetchTaskGroup;
use crate::offsets::run_offset_consumer;

include!(concat!(env!("OUT_DIR"), "/rust_version.rs"));

async fn run_kafka_web(config_path: &str) -> Result<()> {
    let config = config::read_config(config_path)
        .chain_err(|| format!("Unable to load configuration from '{}'", config_path))?;

    
    let replicator_bootstrap_servers = match config.cluster(&config.caching.cluster) {
        Some(cluster) => cluster.bootstrap_servers(),
        None => bail!("Can't find cache cluster {}", config.caching.cluster),
    };
    let clusterConfig =config.cluster(&config.caching.cluster).unwrap();
    let topic_name = &config.caching.topic;
    let replica_writer =
        ReplicaWriter::new(&replicator_bootstrap_servers, topic_name,&clusterConfig).chain_err(|| {
            format!(
                "Replica writer creation failed (brokers: {}, topic: {})",
                replicator_bootstrap_servers, topic_name
            )
        })?;
    
    let mut replica_reader = ReplicaReader::new(&replicator_bootstrap_servers, topic_name, &clusterConfig)
        .chain_err(|| {
            format!(
                "Replica reader creation failed (brokers: {}, topic: {})",
                replicator_bootstrap_servers, topic_name
            )
        })?;

    let cache = Cache::new(replica_writer);

    // Load all the state from Kafka
    let start_time = chrono::Utc::now();
    replica_reader.load_state(cache.alias()).await.chain_err(|| {
        format!(
            "State load failed (brokers: {}, topic: {})",
            replicator_bootstrap_servers, topic_name
        )
    })?;
    let elapsed_sec = chrono::Utc::now()
        .signed_duration_since(start_time)
        .num_milliseconds() as f32
        / 1000f32;
    info!(
        "Processed {} messages in {:.3} seconds ({:.0} msg/s).",
        replica_reader.processed_messages(),
        elapsed_sec,
        replica_reader.processed_messages() as f32 / elapsed_sec
    );

    let executor =
        ThreadPoolExecutor::new(4).chain_err(|| "Failed to start thread pool executor")?;

    // Metadata fetch
    executor.schedule(
        MetadataFetchTaskGroup::new(&cache, &config),
        Duration::from_secs(0),
        Duration::from_secs(config.metadata_refresh),
    );

    // Metrics fetch
    executor.schedule(
        MetricsFetchTaskGroup::new(&cache, &config),
        Duration::from_secs(0),
        Duration::from_secs(config.metrics_refresh),
    );

    // Consumer offsets
    for (cluster_id, cluster_config) in &config.clusters {
        if let Err(e) = run_offset_consumer(cluster_id, cluster_config, &config, &cache) {
            format_error_chain!(e);
        }
    }

    // CACHE EXPIRATION
    let cache_clone = cache.alias();
    let metadata_expiration = config.metadata_refresh * 3;
    executor.schedule_fixed_rate(
        Duration::from_secs(config.metadata_refresh * 2),
        Duration::from_secs(config.metadata_refresh),
        move |_| {
            cache_clone
                .topics
                .remove_expired(Duration::from_secs(metadata_expiration));
            cache_clone
                .brokers
                .remove_expired(Duration::from_secs(metadata_expiration));
            cache_clone
                .groups
                .remove_expired(Duration::from_secs(metadata_expiration));
        },
    );

    let cache_clone = cache.alias();
    let metrics_expiration = config.metrics_refresh * 3;
    executor.schedule_fixed_rate(
        Duration::from_secs(config.metrics_refresh * 2),
        Duration::from_secs(config.metrics_refresh),
        move |_| {
            cache_clone
                .metrics
                .remove_expired(Duration::from_secs(metrics_expiration));
        },
    );

    let cache_clone = cache.alias();
    let offsets_store_duration = config.offsets_store_duration;
    executor.schedule_fixed_rate(
        Duration::from_secs(10),
        Duration::from_secs(120),
        move |_| {
            cache_clone
                .offsets
                .remove_expired(Duration::from_secs(offsets_store_duration));
        },
    );

    web_server::server::run_server(&executor, cache.alias(), &config).await
        .chain_err(|| "Server initialization failed")?;

    Ok(())
}

fn setup_args<'a>() -> ArgMatches<'a> {
    App::new("kafka web interface")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Kafka web interface")
        .arg(
            Arg::with_name("conf")
                .short("c")
                .long("conf")
                .help("Configuration file")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .get_matches()
}
#[tokio::main]
async fn main() {
    let matches = setup_args();

    utils::setup_logger(true, matches.value_of("log-conf"), "%F %T%z");

    let config_path = matches.value_of("conf").unwrap();

    info!("Kafka-view is starting up!");
    if let Err(e) = run_kafka_web(config_path).await {
        format_error_chain!(e);
        std::process::exit(1);
    }
}
