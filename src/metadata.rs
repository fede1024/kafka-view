use chrono::{DateTime, UTC};
use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error as rderror;

use error::*;
use scheduler::{Scheduler, ScheduledTask};
use cache::ReplicatedMap;
use std::time::Duration;
use std::collections::BTreeMap;
use std::sync::Arc;

// TODO: Use structs?
pub type BrokerId = i32;
pub type ClusterId = String;
pub type TopicName = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Partition {
    pub id: i32,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
    pub isr: Vec<BrokerId>,
    pub error: Option<String>
}

impl Partition {
    fn new(id: i32, leader: BrokerId, replicas: Vec<BrokerId>, isr: Vec<BrokerId>, error: Option<String>) -> Partition {
        Partition {
            id: id,
            leader: leader,
            replicas: replicas,
            isr: isr,
            error: error
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Broker {
    pub id: BrokerId,
    pub hostname: String,
    pub port: i32
}

impl Broker {
    fn new(id: BrokerId, hostname: String, port: i32) -> Broker {
        Broker {
            id: id,
            hostname: hostname,
            port: port
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    pub brokers: Vec<Broker>,
    pub topics: BTreeMap<TopicName, Vec<Partition>>,
    pub refresh_time: DateTime<UTC>,
}

impl Metadata {
    fn new(brokers: Vec<Broker>, topics: BTreeMap<TopicName, Vec<Partition>>) -> Metadata {
        Metadata {
            brokers: brokers,
            topics: topics,
            refresh_time: UTC::now(),
        }
    }
}

fn fetch_metadata(consumer: &BaseConsumer<EmptyConsumerContext>, timeout_ms: i32) -> Result<Metadata> {
    let metadata = consumer.fetch_metadata(timeout_ms)
        .chain_err(|| "Failed to fetch metadata from consumer")?;

    let mut brokers = Vec::new();
    for broker in metadata.brokers() {
        brokers.push(Broker::new(broker.id(), broker.host().to_owned(), broker.port()));
    }

    let mut topics = BTreeMap::new();
    for t in metadata.topics() {
        let mut partitions = Vec::with_capacity(t.partitions().len());
        for p in t.partitions() {
            partitions.push(Partition::new(p.id(), p.leader(), p.replicas().to_owned(), p.isr().to_owned(),
                                      p.error().map(|e| rderror::resp_err_description(e))));
        }
        partitions.sort_by(|a, b| a.id.cmp(&b.id));
        topics.insert(t.name().to_owned(), partitions);
    }

    Ok(Metadata::new(brokers, topics))
}


struct MetadataFetcherTask {
    cluster_id: ClusterId,
    brokers: String,
    consumer: Option<BaseConsumer<EmptyConsumerContext>>,
    cache: ReplicatedMap<ClusterId, Arc<Metadata>>,
}

impl MetadataFetcherTask {
    fn new(cluster_id: &ClusterId, brokers: &str, cache: ReplicatedMap<ClusterId, Arc<Metadata>>)
            -> MetadataFetcherTask {
        MetadataFetcherTask {
            cluster_id: cluster_id.to_owned(),
            brokers: brokers.to_owned(),
            consumer: None,
            cache: cache,
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
        debug!("Metadata fetch start");
        let metadata = match self.consumer {
            None => bail!("Consumer not initialized"),
            Some(ref consumer) => fetch_metadata(consumer, 30000)
                .chain_err(|| format!("Metadata fetch failed, cluster: {}", self.cluster_id))?,
        };
        debug!("Metadata fetch end");
        self.cache.insert(self.cluster_id.to_owned(), Arc::new(metadata))
            .chain_err(|| "Failed to create new metadata container to cache")?;
        Ok(())
    }
}

pub struct MetadataFetcher {
    scheduler: Scheduler<ClusterId, MetadataFetcherTask>,
    cache: ReplicatedMap<ClusterId, Arc<Metadata>>,
}

impl MetadataFetcher {
    pub fn new(cache: ReplicatedMap<ClusterId, Arc<Metadata>>, interval: Duration) -> MetadataFetcher {
        MetadataFetcher {
            scheduler: Scheduler::new(interval, 2),
            cache: cache,
        }
    }

    pub fn add_cluster(&mut self, cluster_id: &ClusterId, brokers: &str) -> Result<()> {
        let mut task = MetadataFetcherTask::new(cluster_id, brokers, self.cache.alias());
        task.create_consumer();
        self.scheduler.add_task(cluster_id.to_owned(), task);
        Ok(())
    }
}
