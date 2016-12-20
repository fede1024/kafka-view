extern crate log;
extern crate rdkafka;
extern crate chrono;

use self::chrono::{DateTime, UTC};
use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error as rderror;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::*;

use error::*;
use scheduler::ScheduledTask;
use scheduler::Scheduler;
use std::time::{Duration, Instant};
use std::thread;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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

pub type TopicName = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Metadata {
    brokers: Vec<Broker>,
    topics: HashMap<TopicName, Vec<Partition>>,
    refresh_time: DateTime<UTC>,
}

impl Metadata {
    fn new(brokers: Vec<Broker>, topics: HashMap<TopicName, Vec<Partition>>) -> Metadata {
        Metadata {
            brokers: brokers,
            topics: topics,
            refresh_time: UTC::now(),
        }
    }

    pub fn topic(&self) -> &HashMap<TopicName, Vec<Partition>> {
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

type ClusterId = String;

#[derive(Clone)]
struct SharedMetadataContainer(Arc<RwLock<Option<Arc<Metadata>>>>);

impl SharedMetadataContainer {
    fn new() -> SharedMetadataContainer {
        SharedMetadataContainer(Arc::new(RwLock::new(None)))
    }

    fn set(&self, metadata: Metadata) {
        match self.0.write() {
            Ok(mut content_ref) => *content_ref = Some(Arc::new(metadata)),
            Err(_) => panic!("Poison error!"),
        };
    }

    fn metadata(&self) -> Option<Arc<Metadata>> {
        match self.0.read() {
            Err(_) => None,
            Ok(content_ref) => (*content_ref).clone(),
        }
    }
}

struct MetadataFetcherTask {
    brokers: String,
    consumer: Option<BaseConsumer<EmptyConsumerContext>>,
    container: SharedMetadataContainer,
}

impl MetadataFetcherTask {
    fn new(brokers: &str, container: SharedMetadataContainer) -> MetadataFetcherTask {
        MetadataFetcherTask {
            brokers: brokers.to_owned(),
            consumer: None,
            container: container,
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
        info!("Metadata start {:?}", Instant::now());
        let metadata = match self.consumer {
            None => bail!("Consumer not initialized"),
            Some(ref consumer) => fetch_metadata(consumer, 7000)
                .chain_err(|| "Metadata fetch failed")?,
        };
        // let serialized = serde_json::to_string(&metadata).unwrap();
        // info!("serialized = {}", serialized);
        self.container.set(metadata);
        Ok(())
    }
}

pub struct MetadataFetcher {
    scheduler: Scheduler<ClusterId, MetadataFetcherTask>,
    cache: HashMap<ClusterId, SharedMetadataContainer>,
}

impl MetadataFetcher {
    pub fn new(interval: Duration) -> MetadataFetcher {
        MetadataFetcher {
            scheduler: Scheduler::new(interval),
            cache: HashMap::new(),
        }
    }

    pub fn add_cluster(&mut self, cluster_id: &ClusterId, brokers: &str) {
        let mut metadata_container = SharedMetadataContainer::new();
        let mut task = MetadataFetcherTask::new(brokers, metadata_container.clone());
        task.create_consumer();
        self.scheduler.add_task(cluster_id.to_owned(), task);
        self.cache.insert(cluster_id.to_owned(), metadata_container);
    }

    pub fn get_metadata_copy(&self, cluster_id: &ClusterId) -> Option<Arc<Metadata>> {
        match self.cache.get(cluster_id) {
            Some(container) => container.metadata(),
            None => None,
        }
    }
}
