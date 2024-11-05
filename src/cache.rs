use rand::random;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig};
use crate::config::ClusterConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::{ millis_to_epoch, Timeout};
use serde::de::{Deserialize, DeserializeOwned};
use serde::ser::Serialize;
use serde_json;

use std::borrow::Borrow;
use std::collections::hash_map;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use crate::error::*;
use crate::metadata::{Broker, ClusterId, Group, Partition, TopicName};
use crate::metrics::TopicMetrics;

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
struct WrappedKey(String, String);

impl WrappedKey {
    fn new<'de, K>(cache_name: String, key: &'de K) -> WrappedKey
    where
        K: Serialize + Deserialize<'de>,
    {
        WrappedKey(cache_name, serde_json::to_string(key).unwrap()) //TODO: error handling
    }

    pub fn cache_name(&self) -> &str {
        &self.0
    }

    pub fn serialized_key(&self) -> &str {
        &self.1
    }
}

//
// ********* REPLICA WRITER **********
//

pub struct ReplicaWriter {
    topic_name: String,
    producer: FutureProducer<DefaultClientContext>,
}

impl ReplicaWriter {
    pub fn new(brokers: &str, topic_name: &str,config :&ClusterConfig) -> Result<ReplicaWriter> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("security.protocol", &config.security_protocol.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.service.name", &config.sasl_kerberos_service_name.clone().unwrap_or("".to_owned()) )
            .set("sasl.mechanism", &config.sasl_mechanism.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.principal", &config.sasl_kerberos_principal.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.keytab", &config.sasl_kerberos_keytab.clone().unwrap_or("".to_owned()) )
            
            .set("compression.codec", "gzip")
            .set("message.max.bytes", "10000000")
            .set("api.version.request", "true")
            .create::<FutureProducer<_>>()
            .expect("Producer creation error");

        let writer = ReplicaWriter {
            topic_name: topic_name.to_owned(),
            producer,
        };

        Ok(writer)
    }

    // TODO: use structure for value
    /// Writes a new update into the topic. The name of the replicated map and the key will be
    /// serialized together as key of the message, and the value will be serialized in the payload.
    pub fn update<'de, K, V>(&self, name: &str, key: &'de K, value: &'de V) -> Result<()>
    where
        K: Serialize + Deserialize<'de> + Clone,
        V: Serialize + Deserialize<'de>,
    {
        let serialized_key = serde_json::to_vec(&WrappedKey::new(name.to_owned(), key))
            .chain_err(|| "Failed to serialize key")?;
        let serialized_value =
            serde_json::to_vec(&value).chain_err(|| "Failed to serialize value")?;
        trace!(
            "Serialized update size: key={:.3}KB value={:.3}KB",
            (serialized_key.len() as f64 / 1000f64),
            (serialized_value.len() as f64 / 1000f64)
        );
        let ts = millis_to_epoch(SystemTime::now());
        let _f = self.producer.send::<[u8], [u8],Timeout>(
            FutureRecord{
                topic: self.topic_name.as_str(),
                partition: None,
                key: Some(&serialized_key),
                timestamp:Some(ts),
                headers:None,
                payload:Some(&serialized_value)
            }  ,
            Timeout::After(Duration::from_millis(1000)),
        );
        // _f.wait();  // Uncomment to make production synchronous
        Ok(())
    }

    /// Deletes an element from the specified cache
    pub fn delete<'de, K>(&self, name: &str, key: &'de K) -> Result<()>
    where
        K: Serialize + Deserialize<'de> + Clone,
    {
        let serialized_key = serde_json::to_vec(&WrappedKey::new(name.to_owned(), key))
            .chain_err(|| "Failed to serialize key")?;
        self.write_tombstone(&serialized_key)
    }

    /// Writes a tombstone for the specified message key.
    fn write_tombstone(&self, message_key: &[u8]) -> Result<()> {
        let ts = millis_to_epoch(SystemTime::now());
        let _f = self.producer.send::<[u8], [u8],Timeout>(
            FutureRecord{
                topic: self.topic_name.as_str(),
                partition: None,
                key: Some(message_key),
                timestamp:Some(ts),
                headers:None,
                payload:None
            }  ,
            Timeout::After(Duration::from_millis(1000)),
        );
        Ok(())
    }
}

//
// ********* REPLICA READER **********
//

#[derive(Debug)]
pub enum ReplicaCacheUpdate<'a> {
    Set {
        key: &'a str,
        payload: &'a [u8],
        timestamp: u64,
    },
    Delete {
        key: &'a str,
    },
}

pub trait UpdateReceiver: Send + 'static {
    fn receive_update(&self, name: &str, update: ReplicaCacheUpdate) -> Result<()>;
}

type ReplicaConsumer = StreamConsumer<DefaultConsumerContext>;

pub struct ReplicaReader {
    consumer: ReplicaConsumer,
    brokers: String,
    topic_name: String,
    processed_messages: i64,
}

impl ReplicaReader {
    pub fn new(brokers: &str, topic_name: &str ,config :&ClusterConfig) -> Result<ReplicaReader> {
        let consumer: ReplicaConsumer = ClientConfig::new()
            .set(
                "group.id",
                &format!("kafka_web_cache_reader_{}", random::<i64>()),
            )
            .set("bootstrap.servers", brokers)
            .set("security.protocol", &config.security_protocol.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.service.name", &config.sasl_kerberos_service_name.clone().unwrap_or("".to_owned()) )
            .set("sasl.mechanism", &config.sasl_mechanism.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.principal", &config.sasl_kerberos_principal.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.keytab", &config.sasl_kerberos_keytab.clone().unwrap_or("".to_owned()) )
            
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("queued.min.messages", "10000") // Reduce memory usage
            //.set("fetch.message.max.bytes", "102400")
            .set("api.version.request", "true")
            .set("auto.offset.reset", "smallest")
            .create()
            .chain_err(|| "Consumer creation failed")?;

        //let topic_partition = TopicPartitionList::with_topics(&vec![topic_name]);
        // consumer.assign(&topic_partition)
        consumer
            .subscribe(&[topic_name])
            .chain_err(|| "Can't subscribe to specified topics")?;

        Ok(ReplicaReader {
            consumer,
            brokers: brokers.to_owned(),
            topic_name: topic_name.to_owned(),
            processed_messages: 0,
        })
    }

    pub fn processed_messages(&self) -> i64 {
        self.processed_messages
    }

    pub async fn load_state<R: UpdateReceiver>(&mut self, receiver: R) -> Result<()> {
        info!("Started creating state");
        match self.last_message_per_key().await {
            Err(e) => format_error_chain!(e),
            Ok(state) => {
                for (w_key, message) in state {
                    let update = match message.payload() {
                        Some(payload) => ReplicaCacheUpdate::Set {
                            key: w_key.serialized_key(),
                            payload,
                            timestamp: message
                                .timestamp()
                                .to_millis()
                                .unwrap_or_else(|| millis_to_epoch(SystemTime::now()))
                                as u64,
                        },
                        None => ReplicaCacheUpdate::Delete {
                            key: w_key.serialized_key(),
                        },
                    };
                    if let Err(e) = receiver.receive_update(w_key.cache_name(), update) {
                        format_error_chain!(e);
                    }
                }
            }
        }
        info!("State creation terminated");
        Ok(())
    }

    async fn last_message_per_key(&mut self) -> Result<HashMap<WrappedKey, OwnedMessage>> {
        let mut eof_set = HashSet::new();
        let mut borrowed_state = HashMap::new();
        let mut state = HashMap::new();

        let topic_name = &self.topic_name;
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic_name), Timeout::After(Duration::from_millis(30000)) )
            .chain_err(|| "Failed to fetch metadata")?;

        if metadata.topics().is_empty() {
            warn!(
                "No replicator topic found ({} {})",
                self.brokers, self.topic_name
            );
            return Ok(HashMap::new());
        }
        let topic_metadata = &metadata.topics()[0];
        if topic_metadata.partitions().is_empty() {
            return Ok(state); // Topic is empty and auto created
        }

        

        loop {
            let message = self.consumer.recv().await;
            match message {
                Ok(m) => {
                    self.processed_messages += 1;
                    match parse_message_key(&m).chain_err(|| "Failed to parse message key") {
                        Ok(wrapped_key) => {
                            borrowed_state.insert(wrapped_key, m);
                        }
                        Err(e) => format_error_chain!(e),
                    };
                }
                Err(KafkaError::PartitionEOF(p)) => {
                    eof_set.insert(p);
                }
                Err(e) => {error!("Error while reading from Kafka: {}", e);break;},
                Err(_) => {error!("Stream receive error");break;},
            };
            if borrowed_state.len() >= 10000 {
                for (key, message) in borrowed_state {
                    state.insert(key, message.detach());
                }
                borrowed_state = HashMap::new();
            }
            if eof_set.len() == topic_metadata.partitions().len() {
                break;
            }
        }
        for (key, message) in borrowed_state {
            state.insert(key, message.detach());
        }
        //self.consumer.stop();
        info!("Total unique items in caches: {}", state.len());
        Ok(state)
    }
}

fn parse_message_key(message: &BorrowedMessage) -> Result<WrappedKey> {
    let key_bytes = match message.key() {
        Some(k) => k,
        None => bail!("Empty key found"),
    };

    let wrapped_key = serde_json::from_slice::<WrappedKey>(key_bytes)
        .chain_err(|| "Failed to decode wrapped key")?;
    Ok(wrapped_key)
}

//
// ********** REPLICATED MAP **********
//

#[derive(Clone)]
struct ValueContainer<V> {
    value: V,
    updated: u64, // millis since epoch
}

impl<V> ValueContainer<V> {
    fn new(value: V) -> ValueContainer<V> {
        ValueContainer {
            value,
            updated: millis_to_epoch(SystemTime::now()) as u64,
        }
    }

    fn new_with_timestamp(value: V, timestamp: u64) -> ValueContainer<V> {
        ValueContainer {
            value,
            updated: timestamp,
        }
    }
}

pub struct ReplicatedMap<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + PartialEq + Serialize + DeserializeOwned,
{
    name: String,
    map: Arc<RwLock<HashMap<K, ValueContainer<V>>>>,
    replica_writer: Arc<ReplicaWriter>,
}

impl<K, V> ReplicatedMap<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + PartialEq + Serialize + DeserializeOwned,
{
    pub fn new(name: &str, replica_writer: Arc<ReplicaWriter>) -> ReplicatedMap<K, V> {
        ReplicatedMap {
            name: name.to_owned(),
            map: Arc::new(RwLock::new(HashMap::new())),
            replica_writer,
        }
    }

    pub fn alias(&self) -> ReplicatedMap<K, V> {
        ReplicatedMap {
            name: self.name.clone(),
            map: self.map.clone(),
            replica_writer: self.replica_writer.clone(),
        }
    }

    pub fn keys(&self) -> Vec<K> {
        match self.map.read() {
            Ok(ref cache) => (*cache).keys().cloned().collect::<Vec<_>>(),
            Err(_) => panic!("Poison error"),
        }
    }

    fn receive_update(&self, update: ReplicaCacheUpdate) -> Result<()> {
        match update {
            ReplicaCacheUpdate::Set {
                key,
                payload,
                timestamp,
            } => {
                let key = serde_json::from_str::<K>(key).chain_err(|| "Failed to parse key")?;
                let value =
                    serde_json::from_slice::<V>(payload).chain_err(|| "Failed to parse payload")?;
                self.local_update(key, value, Some(timestamp));
            }
            ReplicaCacheUpdate::Delete { key } => {
                let key = serde_json::from_str::<K>(key).chain_err(|| "Failed to parse key")?;
                self.local_remove(&key);
            }
        }
        Ok(())
    }

    fn local_update(&self, key: K, value: V, timestamp: Option<u64>) {
        let value = if let Some(ts) = timestamp {
            ValueContainer::new_with_timestamp(value, ts)
        } else {
            ValueContainer::new(value)
        };
        match self.map.write() {
            Ok(mut cache) => (*cache).insert(key, value),
            Err(_) => panic!("Poison error"),
        };
    }

    fn local_remove(&self, key: &K) {
        match self.map.write() {
            Ok(mut cache) => (*cache).remove(key),
            Err(_) => panic!("Poison error"),
        };
    }

    pub fn insert(&self, key: K, new_value: V) -> Result<()> {
        let current_value = self.get(&key);
        if current_value.is_none() || current_value.unwrap() != new_value {
            self.replica_writer
                .update(&self.name, &key, &new_value)
                .chain_err(|| "Failed to write cache update")?;
        }
        self.local_update(key, new_value, None);
        Ok(())
    }

    pub fn remove(&self, key: &K) -> Result<()> {
        self.replica_writer
            .delete(&self.name, key)
            .chain_err(|| "Failed to write cache delete")?;
        self.local_remove(key);
        Ok(())
    }

    pub fn remove_expired(&self, max_age: Duration) -> Vec<K> {
        let to_remove = {
            let cache: std::sync::RwLockReadGuard<'_, HashMap<K, ValueContainer<V>>> = self.map.read().unwrap();
            let max_ms = max_age.as_millis() as i64;
            let current_ms = millis_to_epoch(SystemTime::now());
            cache
                .iter()
                .filter(|&(_, v)| (current_ms as i64) - (v.updated as i64) > max_ms)
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>()
        };
        for k in &to_remove {
            if let Err(e) = self.remove(k) {
                format_error_chain!(e);
            }
        }
        to_remove
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self.map.read() {
            Ok(cache) => (*cache).get(key).map(|v| v.value.clone()),
            Err(_) => panic!("Poison error"),
        }
    }

    // TODO: add doc
    pub fn lock_iter<F, R>(&self, f: F) -> R
    where
        for<'a> F: Fn(ReplicatedMapIter<'a, K, V>) -> R,
    {
        match self.map.read() {
            Ok(cache) => {
                let iter = ReplicatedMapIter::new(cache.iter());
                f(iter)
            }
            Err(_) => panic!("Poison error"),
        }
    }

    pub fn count<F>(&self, f: F) -> usize
    where
        F: Fn(&K) -> bool,
    {
        self.lock_iter(|iter| iter.filter(|&(k, _)| f(k)).count())
    }

    pub fn filter_clone<F>(&self, f: F) -> Vec<(K, V)>
    where
        F: Fn(&K) -> bool,
    {
        self.lock_iter(|iter| {
            iter.filter(|&(k, _)| f(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<(K, V)>>()
        })
    }

    //    pub fn filter_clone_v<F>(&self, f: F) -> Vec<V>
    //            where F: Fn(&K) -> bool {
    //        self.lock_iter(|iter| {
    //            iter.filter(|&(k, _)| f(k))
    //                .map(|(_, v)| v.clone())
    //                .collect::<Vec<V>>()
    //        })
    //    }

    pub fn filter_clone_k<F>(&self, f: F) -> Vec<K>
    where
        F: Fn(&K) -> bool,
    {
        self.lock_iter(|iter| {
            iter.filter(|&(k, _)| f(k))
                .map(|(k, _)| k.clone())
                .collect::<Vec<K>>()
        })
    }
}

pub struct ReplicatedMapIter<'a, K, V>
where
    K: 'a,
    V: 'a,
{
    inner: hash_map::Iter<'a, K, ValueContainer<V>>,
}

impl<'a, K, V> ReplicatedMapIter<'a, K, V>
where
    K: 'a,
    V: 'a,
{
    fn new(inner: hash_map::Iter<'a, K, ValueContainer<V>>) -> ReplicatedMapIter<'a, K, V> {
        ReplicatedMapIter { inner }
    }
}

impl<'a, K, V> Iterator for ReplicatedMapIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k, &v.value))
    }
}

//
// ********** CACHE **********
//

/// Metrics for a specific topic
pub type MetricsCache = ReplicatedMap<(ClusterId, TopicName), TopicMetrics>;

/// Broker information
pub type BrokerCache = ReplicatedMap<ClusterId, Vec<Broker>>;

/// Topic and partition information
pub type TopicCache = ReplicatedMap<(ClusterId, TopicName), Vec<Partition>>;

/// Groups
pub type GroupCache = ReplicatedMap<(ClusterId, String), Group>;

/// Consumer group offsets per topic
pub type OffsetsCache = ReplicatedMap<(ClusterId, String, TopicName), Vec<i64>>;

/// Offsets for the internal consumers of the __consumer_offsets topic
pub type InternalConsumerOffsetCache = ReplicatedMap<ClusterId, Vec<i64>>;

pub struct Cache {
    pub metrics: MetricsCache,
    pub offsets: OffsetsCache,
    pub brokers: BrokerCache,
    pub topics: TopicCache,
    pub groups: GroupCache,
    pub internal_offsets: InternalConsumerOffsetCache,
}

impl Cache {
    pub fn new(replica_writer: ReplicaWriter) -> Cache {
        let replica_writer_arc = Arc::new(replica_writer);
        Cache {
            metrics: ReplicatedMap::new("metrics", replica_writer_arc.clone()),
            offsets: ReplicatedMap::new("offsets", replica_writer_arc.clone()),
            brokers: ReplicatedMap::new("brokers", replica_writer_arc.clone()),
            topics: ReplicatedMap::new("topics", replica_writer_arc.clone()),
            groups: ReplicatedMap::new("groups", replica_writer_arc.clone()),
            internal_offsets: ReplicatedMap::new("internal_offsets", replica_writer_arc),
        }
    }

    pub fn alias(&self) -> Cache {
        Cache {
            metrics: self.metrics.alias(),
            offsets: self.offsets.alias(),
            brokers: self.brokers.alias(),
            topics: self.topics.alias(),
            groups: self.groups.alias(),
            internal_offsets: self.internal_offsets.alias(),
        }
    }
}

impl UpdateReceiver for Cache {
    fn receive_update(&self, cache_name: &str, update: ReplicaCacheUpdate) -> Result<()> {
        match cache_name {
            "metrics" => self.metrics.receive_update(update),
            "offsets" => self.offsets.receive_update(update),
            "brokers" => self.brokers.receive_update(update),
            "topics" => self.topics.receive_update(update),
            "groups" => self.groups.receive_update(update),
            "internal_offsets" => self.internal_offsets.receive_update(update),
            _ => bail!("Unknown cache name: {}", cache_name),
        }
    }
}
