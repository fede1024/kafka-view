extern crate log;
extern crate rdkafka;

extern crate serde_json;

use error::*;

use scheduler::ScheduledTask;
use scheduler::Scheduler;

use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error as rderror;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::*;

use std::time::Duration;
use std::thread;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
struct Partition {
    id: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    error: Option<String>
}

impl Partition {
    fn new(id: i32, leader: i32, replicas: Vec<i32>, isr: Vec<i32>, error: Option<String>) -> Partition {
        Partition {
            id: id,
            leader: leader,
            replicas: replicas,
            isr: isr,
            error: error
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Broker {
    id: i32,
    host: String,
    port: i32
}

impl Broker {
    fn new(id: i32, host: String, port: i32) -> Broker {
        Broker {
            id: id,
            host: host,
            port: port
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
    brokers: Vec<Broker>,
    topics: HashMap<String, Vec<Partition>>,
}

impl Metadata {
    fn new(brokers: Vec<Broker>, topics: HashMap<String, Vec<Partition>>) -> Metadata {
        Metadata {
            brokers: brokers,
            topics: topics,
        }
    }
}

fn fetch_metadata(consumer: &BaseConsumer<EmptyConsumerContext>) -> Result<Metadata> {
    let metadata = consumer.fetch_metadata(10000)
        .chain_err(|| "Failed to fetch metadata from consumer")?;

    let mut brokers = Vec::new();
    for broker in metadata.brokers() {
        brokers.push(Broker::new(broker.id(), broker.host().to_owned(), broker.port()));
    }

    let mut topics = HashMap::new();
    for t in metadata.topics() {
        let mut topic = Vec::with_capacity(t.partitions().len());
        for p in t.partitions() {
            topic.push(Partition::new(p.id(), p.leader(), p.replicas().to_owned(), p.isr().to_owned(),
                                      p.error().map(|e| rderror::resp_err_description(e))));
        }
        topics.insert(t.name().to_owned(), topic);
    }

    Ok(Metadata::new(brokers, topics))
}

struct MetadataFetcherTask {
    brokers: String,
    consumer: Option<BaseConsumer<EmptyConsumerContext>>,
}

impl MetadataFetcherTask {
    fn new(brokers: &str) -> MetadataFetcherTask {
        MetadataFetcherTask {
            brokers: brokers.to_owned(),
            consumer: None,
        }
    }

    fn create_consumer(&mut self) {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create::<BaseConsumer<_>>()
            .expect("Consumer creation failed");
        self.consumer = Some(consumer);
    }
}

impl ScheduledTask for MetadataFetcherTask {
    fn run(&self) -> Result<()> {
        let metadata = match self.consumer {
            None => bail!("Consumer not initialized"),
            Some(ref consumer) => fetch_metadata(consumer)
                .chain_err(|| "Metadata fetch failed")?,
        };
        let serialized = serde_json::to_string(&metadata).unwrap();
        info!("serialized = {}", serialized);
        Ok(())
    }
}

pub struct MetadataFetcher {
    scheduler: Scheduler<String, MetadataFetcherTask>
}

impl MetadataFetcher {
    pub fn new(interval: Duration) -> MetadataFetcher {
        MetadataFetcher {
            scheduler: Scheduler::new(interval)
        }
    }

    pub fn add_cluster(&mut self, cluster_name: &str, brokers: &str) {
        let mut task = MetadataFetcherTask::new(brokers);
        task.create_consumer();
        self.scheduler.add_task(cluster_name.to_owned(), task);
    }
}
