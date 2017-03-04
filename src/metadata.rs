use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error as rderror;

use error::*;
use scheduler::{Scheduler, ScheduledTask};
use cache::ReplicatedMap;
use std::time::Duration;

// TODO: Use structs?
pub type BrokerId = i32;
pub type ClusterId = String;
pub type TopicName = String;

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

fn fetch_groups(consumer: &BaseConsumer<EmptyConsumerContext>, timeout_ms: i32) -> Result<Vec<Group>> {
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
    bootstrap_servers: String,
    consumer: Option<BaseConsumer<EmptyConsumerContext>>,
    broker_cache: ReplicatedMap<ClusterId, Vec<Broker>>,
    topic_cache: ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>,
    group_cache: ReplicatedMap<(ClusterId, String), Group>
}

impl MetadataFetcherTask {
    fn new(
        cluster_id: &ClusterId,
        bootstrap_servers: &str,
        broker_cache: ReplicatedMap<ClusterId, Vec<Broker>>,
        topic_cache: ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>,
        group_cache: ReplicatedMap<(ClusterId, String), Group>
    ) -> MetadataFetcherTask {
        MetadataFetcherTask {
            cluster_id: cluster_id.to_owned(),
            bootstrap_servers: bootstrap_servers.to_owned(),
            consumer: None,
            broker_cache: broker_cache,
            topic_cache: topic_cache,
            group_cache: group_cache,
        }
    }

    fn create_consumer(&mut self) {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create::<BaseConsumer<_>>()
            .expect("Consumer creation failed");
        self.consumer = Some(consumer);
    }
}

impl ScheduledTask for MetadataFetcherTask {
    fn run(&self) -> Result<()> {
        let ref consumer = self.consumer.as_ref().ok_or_else(|| "Consumer not initialized")?;
        let metadata = self.consumer.as_ref().unwrap().fetch_metadata(30000)
            .chain_err(|| "Failed to fetch metadata from consumer")?;
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
            // topics.insert(t.name().to_owned(), partitions);
            self.topic_cache.insert((self.cluster_id.to_owned(), topic.name().to_owned()), partitions)
                .chain_err(|| "Failed to insert broker information in cache")?;
        }

        // Fetch groups
        for group in fetch_groups(consumer, 30000)? {
            self.group_cache.insert((self.cluster_id.to_owned(), group.name.to_owned()), group);
        }

        Ok(())
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
            scheduler: Scheduler::new(interval, 2),
            broker_cache: broker_cache,
            topic_cache: topic_cache,
            group_cache: group_cache,
        }
    }

    pub fn add_cluster(&mut self, cluster_id: &ClusterId, bootstrap_servers: &str) -> Result<()> {
        let mut task = MetadataFetcherTask::new(
            cluster_id, bootstrap_servers, self.broker_cache.alias(),
            self.topic_cache.alias(), self.group_cache.alias());
        task.create_consumer();
        // TODO: scheduler should receive a lambda
        self.scheduler.add_task(cluster_id.to_owned(), task);
        Ok(())
    }
}
