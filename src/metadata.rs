use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error as rderror;

use cache::{Cache, ReplicatedMap};
use config::{ClusterConfig, Config};
use error::*;
use scheduler::{Scheduler, ScheduledTask};
use scheduled_executor::{Executor, Handle, TaskGroup};

use std::time::Duration;
use std::borrow::Borrow;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;


pub type MetadataConsumer = BaseConsumer<EmptyConsumerContext>;

lazy_static! {
    pub static ref CONSUMERS: MetadataConsumerCache = MetadataConsumerCache::new();
}

pub struct MetadataConsumerCache {
    consumers: RwLock<HashMap<ClusterId, Arc<MetadataConsumer>>>,
}

impl MetadataConsumerCache {
    pub fn new() -> MetadataConsumerCache {
        MetadataConsumerCache {
            consumers: RwLock::new(HashMap::new())
        }
    }

    pub fn get(&self, cluster_id: &ClusterId) -> Option<Arc<MetadataConsumer>> {
        match self.consumers.read() {
            Ok(consumers) => (*consumers).get(cluster_id).cloned(),
            Err(_) => panic!("Poison error while reading consumer from cache")
        }
    }

    pub fn get_err(&self, cluster_id: &ClusterId) -> Result<Arc<MetadataConsumer>> {
        self.get(cluster_id).ok_or(ErrorKind::MissingConsumerError(cluster_id.clone()).into())
    }

    pub fn get_or_initialize(&self, cluster_id: &ClusterId, config: &ClusterConfig) -> Result<Arc<MetadataConsumer>> {
        if let Some(consumer) = self.get(cluster_id) {
            return Ok(consumer);
        }

        debug!("Creating metadata consumer for {}", cluster_id);
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers())
            .set("api.version.request", "true")
            .create::<MetadataConsumer>()
            .chain_err(|| format!("Consumer creation failed for {}", cluster_id))?;

        let consumer_arc = Arc::new(consumer);
        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(cluster_id.clone(), consumer_arc.clone()),
            Err(_) => panic!("Poison error while writing consumer to cache")
        };

        Ok(consumer_arc)
    }
}

// TODO: Use structs?
pub type BrokerId = i32;
pub type TopicName = String;

#[derive(Eq, PartialEq, Hash, Clone, Debug, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a str> for ClusterId {
    fn from(id: &'a str) -> ClusterId {
        ClusterId(id.to_owned())
    }
}

impl From<String> for ClusterId {
    fn from(id: String) -> ClusterId {
        ClusterId(id)
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

//
// ********** METADATA **********
//

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


//
// ********** GROUPS **********
//


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>
}

fn fetch_groups(consumer: &MetadataConsumer, timeout_ms: i32) -> Result<Vec<Group>> {
    let group_list = consumer.fetch_group_list(None, timeout_ms)
        .chain_err(|| "Failed to fetch consumer group list")?;

    let mut groups = Vec::new();
    for rd_group in group_list.groups() {
        let members = rd_group.members().iter()
            .map(|m| GroupMember {
                id: m.id().to_owned(),
                client_id: m.client_id().to_owned(),
                client_host: m.client_host().to_owned()
            })
            .collect::<Vec<_>>();
        groups.push(Group {
            name: rd_group.name().to_owned(),
            state: rd_group.state().to_owned(),
            members: members
        })
    }
    Ok(groups)
}


// TODO: remove and use MetadataFetcher directly
struct MetadataFetcherTask {
    cluster_id: ClusterId,
    consumer: Arc<MetadataConsumer>,
    broker_cache: ReplicatedMap<ClusterId, Vec<Broker>>,
    topic_cache: ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>,
    group_cache: ReplicatedMap<(ClusterId, String), Group>
}

impl MetadataFetcherTask {
    fn new(
        cluster_id: &ClusterId,
        consumer: Arc<MetadataConsumer>,
        broker_cache: ReplicatedMap<ClusterId, Vec<Broker>>,
        topic_cache: ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>,
        group_cache: ReplicatedMap<(ClusterId, String), Group>
    ) -> MetadataFetcherTask {
        MetadataFetcherTask {
            cluster_id: cluster_id.to_owned(),
            consumer: consumer,
            broker_cache: broker_cache,
            topic_cache: topic_cache,
            group_cache: group_cache,
        }
    }
}

impl ScheduledTask for MetadataFetcherTask {
    fn run(&self) -> Result<()> {
        let metadata = self.consumer.fetch_metadata(60000)
            .chain_err(|| format!("Failed to fetch metadata from {}", self.cluster_id))?;
        let mut brokers = Vec::new();
        for broker in metadata.brokers() {
            brokers.push(Broker::new(broker.id(), broker.host().to_owned(), broker.port()));
        }
        self.broker_cache.insert(self.cluster_id.to_owned(), brokers)
            .chain_err(|| "Failed to insert broker information in cache")?;

        for topic in metadata.topics() {
            let mut partitions = Vec::with_capacity(topic.partitions().len());
            for p in topic.partitions() {
                partitions.push(Partition::new(p.id(), p.leader(), p.replicas().to_owned(), p.isr().to_owned(),
                                               p.error().map(|e| rderror::resp_err_description(e))));
            }
            partitions.sort_by(|a, b| a.id.cmp(&b.id));
            self.topic_cache.insert((self.cluster_id.to_owned(), topic.name().to_owned()), partitions)
                .chain_err(|| "Failed to insert broker information in cache")?;
        }

        // Fetch groups
        for group in fetch_groups(self.consumer.as_ref(), 30000)? {
            self.group_cache.insert((self.cluster_id.to_owned(), group.name.to_owned()), group);
        }

        Ok(())
    }
}

pub struct MetadataFetchTaskGroup {
    cache: Cache,
    config: Config,
}

impl MetadataFetchTaskGroup {
    pub fn new(cache: Cache, config: Config) -> MetadataFetchTaskGroup {
        MetadataFetchTaskGroup {
            cache: cache,
            config: config,
        }
    }
}

impl TaskGroup for MetadataFetchTaskGroup {
    type TaskId = ClusterId;

    fn get_tasks(&self) -> Vec<ClusterId> {
        self.config.clusters.keys().cloned().collect::<Vec<_>>()
    }

    fn execute(&self, cluster_id: ClusterId, _handle: Option<Handle>) {
        let consumer = CONSUMERS.get_or_initialize(&cluster_id, self.config.cluster(&cluster_id).unwrap());
        debug!("Fetch metadata for {}", cluster_id);
    }
}


pub struct MetadataFetcher {
    scheduler: Scheduler<ClusterId, MetadataFetcherTask>,
    broker_cache: ReplicatedMap<ClusterId, Vec<Broker>>,
    topic_cache: ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>,
    group_cache: ReplicatedMap<(ClusterId, String), Group>
}

impl MetadataFetcher {
    pub fn new(
        broker_cache: ReplicatedMap<ClusterId, Vec<Broker>>,
        topic_cache: ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>,
        group_cache: ReplicatedMap<(ClusterId, String), Group>,
        interval: Duration
    ) -> MetadataFetcher {
        MetadataFetcher {
            scheduler: Scheduler::new(interval, 4),
            broker_cache: broker_cache,
            topic_cache: topic_cache,
            group_cache: group_cache,
        }
    }

    pub fn add_cluster(&mut self, cluster_id: &ClusterId, cluster_config: &ClusterConfig) -> Result<()> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &cluster_config.bootstrap_servers())
            .set("api.version.request", "true")
            .create::<MetadataConsumer>()
            .expect("Consumer creation failed");

        let consumer_arc = Arc::new(consumer);

//        CONSUMERS.write()
//            .map(|mut cache| (*cache).insert(cluster_id.clone(), consumer_arc.clone()))
//            .map_err(|_| ErrorKind::PoisonError("adding consumer to cache".to_owned()))?;

        let mut task = MetadataFetcherTask::new(
            cluster_id, consumer_arc, self.broker_cache.alias(),
            self.topic_cache.alias(), self.group_cache.alias());

        // TODO: scheduler should receive a lambda
        self.scheduler.add_task(cluster_id.to_owned(), task);
        Ok(())
    }
}
