use chrono::{DateTime, UTC};
use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error as rderror;

use error::*;
use scheduler::ScheduledTask;
use scheduler::Scheduler;
use cache::{Cache, ReplicatedCache};
use std::time::Duration;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Partition {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub error: Option<String>
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Broker {
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

pub type TopicName = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    brokers: Vec<Broker>,
    topics: BTreeMap<TopicName, Vec<Partition>>,
    refresh_time: DateTime<UTC>,
}

impl Metadata {
    fn new(brokers: Vec<Broker>, topics: BTreeMap<TopicName, Vec<Partition>>) -> Metadata {
        Metadata {
            brokers: brokers,
            topics: topics,
            refresh_time: UTC::now(),
        }
    }

    pub fn topics(&self) -> &BTreeMap<TopicName, Vec<Partition>> {
        &self.topics
    }

    pub fn refresh_time(&self) -> DateTime<UTC> {
        self.refresh_time
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

pub type ClusterId = String;

struct MetadataFetcherTask {
    cluster_id: ClusterId,
    brokers: String,
    consumer: Option<BaseConsumer<EmptyConsumerContext>>,
    cache: Cache<ClusterId, Metadata>,
}

impl MetadataFetcherTask {
    fn new(cluster_id: &ClusterId, brokers: &str, cache: Cache<ClusterId, Metadata>) -> MetadataFetcherTask {
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
        self.cache.insert(self.cluster_id.to_owned(), metadata)
            .chain_err(|| "Failed to create new metadata container to cache")?;
        Ok(())
    }
}

pub struct MetadataFetcher {
    scheduler: Scheduler<ClusterId, MetadataFetcherTask>,
    cache: Cache<ClusterId, Metadata>,
}

impl MetadataFetcher {
    pub fn new(cache: Cache<ClusterId, Metadata>, interval: Duration) -> MetadataFetcher {
        MetadataFetcher {
            scheduler: Scheduler::new(interval),
            cache: cache,
        }
    }

    pub fn add_cluster(&mut self, cluster_id: &ClusterId, brokers: &str) -> Result<()> {
        let mut task = MetadataFetcherTask::new(cluster_id, brokers, self.cache.clone());
        task.create_consumer();
        self.scheduler.add_task(cluster_id.to_owned(), task);
        Ok(())
    }

    // pub fn clusters(&self) -> Vec<ClusterId> {
    //     self.cache.keys()
    // }

    // pub fn get_metadata(&self, cluster_id: &ClusterId) -> Option<Arc<Metadata>> {
    //     self.cache.get(cluster_id)
    // }
}
