use byteorder::{BigEndian, ReadBytesExt};
use rdkafka::bindings::rd_kafka_err2str;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error as rderror;
use scheduled_executor::TaskGroup;

use crate::cache::Cache;
use crate::config::{ClusterConfig, Config};
use crate::error::*;
use crate::utils::read_str;

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::Cursor;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use rdkafka::util::{cstr_to_owned, Timeout};

pub type MetadataConsumer = BaseConsumer<DefaultConsumerContext>;

lazy_static! {
    pub static ref CONSUMERS: MetadataConsumerCache = MetadataConsumerCache::new();
}

pub struct MetadataConsumerCache {
    consumers: RwLock<HashMap<ClusterId, Arc<MetadataConsumer>>>,
}

impl MetadataConsumerCache {
    pub fn new() -> MetadataConsumerCache {
        MetadataConsumerCache {
            consumers: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, cluster_id: &ClusterId) -> Option<Arc<MetadataConsumer>> {
        match self.consumers.read() {
            Ok(consumers) => (*consumers).get(cluster_id).cloned(),
            Err(_) => panic!("Poison error while reading consumer from cache"),
        }
    }

    pub fn get_err(&self, cluster_id: &ClusterId) -> Result<Arc<MetadataConsumer>> {
        self.get(cluster_id)
            .ok_or_else(|| ErrorKind::MissingConsumerError(cluster_id.clone()).into())
    }

    pub fn get_or_init(
        &self,
        cluster_id: &ClusterId,
        config: &ClusterConfig,
    ) -> Result<Arc<MetadataConsumer>> {
        if let Some(consumer) = self.get(cluster_id) {
            return Ok(consumer);
        }

        debug!("Creating metadata consumer for {}", cluster_id);
        info!("security_protocol={}",&config.security_protocol.clone().unwrap_or("".to_owned()) );

        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers())
            .set("security.protocol", &config.security_protocol.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.service.name", &config.sasl_kerberos_service_name.clone().unwrap_or("".to_owned()) )
            .set("sasl.mechanism", &config.sasl_mechanism.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.principal", &config.sasl_kerberos_principal.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.keytab", &config.sasl_kerberos_keytab.clone().unwrap_or("".to_owned()) )
            
            .set("api.version.request", "true")
            .create::<MetadataConsumer>()
            .chain_err(|| format!("Consumer creation failed for {}", cluster_id))?;

        let consumer_arc = Arc::new(consumer);
        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(cluster_id.clone(), consumer_arc.clone()),
            Err(_) => panic!("Poison error while writing consumer to cache"),
        };

        Ok(consumer_arc)
    }
}

// TODO: Use structs?
pub type BrokerId = i32;
pub type TopicName = String;
pub type TopicPartition = (TopicName, i32);

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

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Partition {
    pub id: i32,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
    pub isr: Vec<BrokerId>,
    pub error: Option<String>,
}

impl Partition {
    fn new(
        id: i32,
        leader: BrokerId,
        mut replicas: Vec<BrokerId>,
        mut isr: Vec<BrokerId>,
        error: Option<String>,
    ) -> Partition {
        replicas.sort();
        isr.sort();
        Partition {
            id,
            leader,
            replicas,
            isr,
            error,
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Broker {
    pub id: BrokerId,
    pub hostname: String,
    pub port: i32,
}

impl Broker {
    fn new(id: BrokerId, hostname: String, port: i32) -> Broker {
        Broker { id, hostname, port }
    }
}

//
// ********** REASSIGNMENT **********
//

#[derive(Debug, Deserialize, Serialize)]
pub struct Reassignment {
    pub partitions: Vec<PartitionReassignment>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionReassignment {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<BrokerId>,
    // replicas: &'a [BrokerId],  // This cannot be deserialized with zero-copy :(
}

//
// ********** GROUPS **********
//

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
    #[serde(default)]
    pub assignments: Vec<MemberAssignment>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Group {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
}

fn parse_member_assignment(payload_rdr: &mut Cursor<&[u8]>) -> Result<Vec<MemberAssignment>> {
    let _version = payload_rdr
        .read_i16::<BigEndian>()
        .chain_err(|| "Failed to parse value version")?;
    let assign_len = payload_rdr
        .read_i32::<BigEndian>()
        .chain_err(|| "Failed to parse assigment length")?;
    let mut assigns = Vec::with_capacity(assign_len as usize);
    for _ in 0..assign_len {
        let topic = read_str(payload_rdr)
            .chain_err(|| "Failed to parse assignment topic name")?
            .to_owned();
        let partition_len = payload_rdr
            .read_i32::<BigEndian>()
            .chain_err(|| "Failed to parse assignment partition length")?;
        let mut partitions = Vec::with_capacity(partition_len as usize);
        for _ in 0..partition_len {
            let partition = payload_rdr
                .read_i32::<BigEndian>()
                .chain_err(|| "Failed to parse assignment partition")?;
            partitions.push(partition);
        }
        assigns.push(MemberAssignment { topic, partitions })
    }
    Ok(assigns)
}

fn fetch_groups(consumer: &MetadataConsumer, timeout_ms: i32) -> Result<Vec<Group>> {
    let group_list = consumer
        .fetch_group_list(None,Timeout::After(Duration::from_millis(timeout_ms as u64)))
        .chain_err(|| "Failed to fetch consumer group list")?;
    info!("groups size={}",group_list.groups().len());
    let mut groups = Vec::new();
    for rd_group in group_list.groups() {
        let members = rd_group
            .members()
            .iter()
            .map(|m| {
                let mut assigns = Vec::new();
                if rd_group.protocol_type() == "consumer" {
                    if let Some(assignment) = m.assignment() {
                        let mut payload_rdr = Cursor::new(assignment);
                        assigns = parse_member_assignment(&mut payload_rdr)
                            .expect("Parse member assignment failed");
                    }
                }
                GroupMember {
                    id: m.id().to_owned(),
                    client_id: m.client_id().to_owned(),
                    client_host: m.client_host().to_owned(),
                    assignments: assigns,
                }
            })
            .collect::<Vec<_>>();
        groups.push(Group {
            name: rd_group.name().to_owned(),
            state: rd_group.state().to_owned(),
            members,
        })
    }
    Ok(groups)
}

pub struct MetadataFetchTaskGroup {
    cache: Cache,
    config: Config,
}

impl MetadataFetchTaskGroup {
    pub fn new(cache: &Cache, config: &Config) -> MetadataFetchTaskGroup {
        MetadataFetchTaskGroup {
            cache: cache.alias(),
            config: config.clone(),
        }
    }

    fn fetch_data(&self, consumer: Arc<MetadataConsumer>, cluster_id: &ClusterId) -> Result<()> {
        let metadata = consumer
            .fetch_metadata(None,   Timeout::After(Duration::from_millis(120_000)))
            .chain_err(|| format!("Failed to fetch metadata from {}", cluster_id))?;

        // Brokers
        let mut brokers = Vec::new();
        for broker in metadata.brokers() {
            brokers.push(Broker::new(
                broker.id(),
                broker.host().to_owned(),
                broker.port(),
            ));
        }
        self.cache
            .brokers
            .insert(cluster_id.to_owned(), brokers)
            .chain_err(|| "Failed to insert broker information in cache")?;

        // Topics
        for topic in metadata.topics() {
            let mut partitions = Vec::with_capacity(topic.partitions().len());
            for p in topic.partitions() {
                let err_descr = p
                    .error()
                    .map(|e| unsafe { std::ffi::CStr::from_ptr( rd_kafka_err2str(e)).to_str().unwrap_or_default().to_owned()});
                let partition = Partition::new(
                    p.id(),
                    p.leader(),
                    p.replicas().to_owned(),
                    p.isr().to_owned(),
                    err_descr,
                );
                partitions.push(partition);
            }
            partitions.sort_by(|a, b| a.id.cmp(&b.id));
            // TODO: do not update if it's already there?
            self.cache
                .topics
                .insert((cluster_id.clone(), topic.name().to_owned()), partitions)
                .chain_err(|| "Failed to insert broker information in cache")?;
        }

        // Groups
        for group in fetch_groups(consumer.as_ref(), 30000)? {
            self.cache
                .groups
                .insert((cluster_id.clone(), group.name.to_owned()), group)?;
        }

        Ok(())
    }
}

impl TaskGroup for MetadataFetchTaskGroup {
    type TaskId = ClusterId;

    fn get_tasks(&self) -> Vec<ClusterId> {
        self.config.clusters.keys().cloned().collect::<Vec<_>>()
    }

    fn execute(&self, cluster_id: ClusterId) {
        match CONSUMERS.get_or_init(&cluster_id, self.config.cluster(&cluster_id).unwrap()) {
            Ok(consumer) => {
                if let Err(e) = self.fetch_data(consumer, &cluster_id) {
                    format_error_chain!(e);
                }
            }
            Err(e) => format_error_chain!(e),
        }
    }
}
