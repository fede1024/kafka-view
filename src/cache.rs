use futures::stream::Stream;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::consumer::stream_consumer::{MessageStream, StreamConsumer};
use rdkafka::consumer::{Consumer, EmptyConsumerContext};
use rdkafka::producer::{FutureProducer, EmptyProducerContext, FutureProducerTopic};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use serde::de::Deserialize;
use serde::ser::Serialize;
use serde_cbor;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use error::*;
use utils::format_error_chain;
use metadata::Metadata;

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
struct WrappedKey(String, Vec<u8>);

impl WrappedKey {
    fn new<K>(cache_name: String, key: &K) -> WrappedKey
            where K: Serialize + Deserialize {
        WrappedKey(cache_name, serde_cbor::to_vec(key).unwrap())  //TODO: error handling
    }

    pub fn cache_name(&self) -> &str {
        &self.0
    }

    pub fn serialized_key(&self) -> &[u8] {
        &self.1
    }
}

//
// ********* REPLICA WRITER **********
//

type ReplicatorProducer = FutureProducer<EmptyProducerContext>;
type ReplicatorTopic = FutureProducerTopic<EmptyProducerContext>;

pub struct ReplicaWriter {
//    brokers: String,
//    topic_name: String,
    producer_topic: ReplicatorTopic,
}

impl ReplicaWriter {
    pub fn new(brokers: &str, topic_name: &str) -> Result<ReplicaWriter> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("compression.codec", "gzip")
            .set("message.max.bytes", "10000000")
            .create::<FutureProducer<_>>()
            .expect("Producer creation error");

        producer.start();

        let topic = producer.get_topic(topic_name, &TopicConfig::new())
            .expect("Topic creation error");

        let writer = ReplicaWriter {
//            brokers: brokers.to_owned(),
//            topic_name: topic_name.to_owned(),
            producer_topic: topic,
        };

        Ok(writer)
    }

    // TODO: use structure for value
    pub fn write_update<K, V>(&self, name: &str, key: &K, value: &V) -> Result<()>
            where K: Serialize + Deserialize + Clone,
                  V: Serialize + Deserialize {
        let serialized_key = serde_cbor::to_vec(&WrappedKey::new(name.to_owned(), key))
            .chain_err(|| "Failed to serialize key")?;
        let serialized_value = serde_cbor::to_vec(&value)
            .chain_err(|| "Failed to serialize value")?;
        trace!("Serialized value size: {}", serialized_value.len());
        let _f = self.producer_topic.send_copy(None, Some(&serialized_value), Some(&serialized_key))
            .chain_err(|| "Failed to produce message")?;
        // _f.wait();  // Uncomment to make production synchronous
        Ok(())
    }
}

//
// ********* REPLICA READER **********
//

pub trait UpdateReceiver: Send + 'static {
    fn update(&self, name: &str, key_bytes: &[u8], msg: Message) -> Result<()>;
}

type ReplicaConsumer = StreamConsumer<EmptyConsumerContext>;

pub struct ReplicaReader {
    consumer: ReplicaConsumer,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl ReplicaReader {
    pub fn new(brokers: &str, topic_name: &str) -> Result<ReplicaReader> {
        let mut consumer: ReplicaConsumer = ClientConfig::new()
            .set("group.id", "kafka_web_replica_reader")  // TODO: make random
            .set("bootstrap.servers", brokers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set_default_topic_config(
                TopicConfig::new()
                .set("auto.offset.reset", "smallest")
                .finalize())
            .create()
            .chain_err(|| "Consumer creation failed")?;

        //let topic_partition = TopicPartitionList::with_topics(&vec![topic_name]);
        // TODO: does this handle partition creation?
        // consumer.assign(&topic_partition)
        consumer.subscribe(&vec![topic_name])
            .chain_err(|| "Can't subscribe to specified topics")?;

        Ok(ReplicaReader {
            consumer: consumer,
            thread_handle: None,
        })
    }

    pub fn start<R: UpdateReceiver>(&mut self, rec: R) -> Result<()> {
        let stream = self.consumer.start();
        let handle = thread::Builder::new()
            .name("replica consumer".to_string())
            .spawn(move || {
                match rebuild_state(stream) {
                    Err(e) => format_error_chain(e),
                    Ok(state) => {
                        for (wrapped_key, message) in state {
                            rec.update(wrapped_key.cache_name(), wrapped_key.serialized_key(), message);
                        }
                    }
                };
            })
            .chain_err(|| "Failed to start polling thread")?;
        self.thread_handle = Some(handle);
        Ok(())
    }
}

fn rebuild_state(stream: MessageStream) -> Result<HashMap<WrappedKey, Message>> {
    let mut EOF_set = HashSet::new();
    let mut state: HashMap<WrappedKey, Message> = HashMap::new();

    trace!("Started creating state");
    for message in stream.wait() {
        match message {
            Ok(m) => { update_startup_map(m, &mut state).map_err(format_error_chain); },
            Err(KafkaError::PartitionEOF(p)) => { EOF_set.insert(p); () },
            Err(e) => error!("Cosumption error: {}", e),
        };
        if EOF_set.len() == 3 { // TODO: make configurable
            break; // TODO: should stop consumer
        }
    }
    trace!("State creation terminated");

    Ok(state)
}

fn update_startup_map(message: Message, state: &mut HashMap<WrappedKey, Message>) -> Result<()> {
    let wrapped_key = parse_message_key(&message)
        .chain_err(|| "Failed to parse message key")?;
    state.insert(wrapped_key, message);
    Ok(())
}

fn parse_message_key(message: &Message) -> Result<WrappedKey> {
    let key_bytes = match message.key() {
        Some(k) => k,
        None => bail!("Empty key found"),
    };

    let wrapped_key = serde_cbor::from_slice::<WrappedKey>(key_bytes)
        .chain_err(|| "Failed to decode wrapped key")?;
    Ok(wrapped_key)
}

// pub trait ReplicatedCache {
//     type Key: Serialize + Deserialize;
//     type Value: Serialize + Deserialize;
//
//     fn new(SharedReplicaWriter, &str) -> Self;
//     fn name(&self) -> &str;
//     fn insert(&self, Self::Key, Self::Value) -> Result<Arc<Self::Value>>;
//     fn get(&self, &Self::Key) -> Option<Arc<Self::Value>>;
//     fn keys(&self) -> Vec<Self::Key>;
// }


// impl<V> ValueContainer<V>
//   where K: Serialize + Deserialize {
//     fn new(id: i32, value: V) -> WrappedKey<V> {
//         ValueContainer {
//             id: id,
//             value: value,
//         }
//     }
// }


// TODO? use inner object with one Arc?
pub struct ReplicatedMap<K, V>
        where K: Eq + Hash + Clone + Serialize + Deserialize,
              V: Clone + Serialize + Deserialize {
    name: String,
    cache_lock: Arc<RwLock<HashMap<K, V>>>,
    replica_writer: Arc<ReplicaWriter>,
}

impl<K, V> ReplicatedMap<K, V>
        where K: Eq + Hash + Clone + Serialize + Deserialize,
              V: Clone + Serialize + Deserialize {
    pub fn new(name: &str, replica_writer: Arc<ReplicaWriter>) -> ReplicatedMap<K, V> {
        ReplicatedMap {
            name: name.to_owned(),
            cache_lock: Arc::new(RwLock::new(HashMap::new())),
            replica_writer: replica_writer,
        }
    }

    pub fn alias(&self) -> ReplicatedMap<K, V> {
        ReplicatedMap {
            name: self.name.clone(),
            cache_lock: self.cache_lock.clone(),
            replica_writer: self.replica_writer.clone(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn keys(&self) -> Vec<K> {
        match self.cache_lock.read() {
            Ok(ref cache) => (*cache).keys().cloned().collect::<Vec<_>>(),
            Err(_) => panic!("Poison error"),
        }
    }

    pub fn sync_value_update(&self, key: K, value: V) {
        match self.cache_lock.write() {
            Ok(mut cache) => (*cache).insert(key, value),
            Err(_) => panic!("Poison error"),
        };
    }

    pub fn insert(&self, key: K, value: V) -> Result<()> {
        self.replica_writer.write_update(&self.name, &key, &value)
            .chain_err(|| "Failed to write cache update")?;
        self.sync_value_update(key, value);
        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<V> {
        match self.cache_lock.read() {
            Ok(cache) => (*cache).get(key).map(|v| v.clone()),
            Err(_) => panic!("Poison error"),
        }
    }
}

//
// ********** CACHE **********
//

pub type MetadataCache = ReplicatedMap<String, Arc<Metadata>>;
pub type WatermarkCache = ReplicatedMap<String, (i64, i64)>;

pub struct Cache {
    pub metadata: MetadataCache,
    pub watermarks: WatermarkCache,
}

impl Cache {
    pub fn new(replica_writer: ReplicaWriter) -> Cache {
        let replica_writer_arc = Arc::new(replica_writer);
        Cache {
            metadata: ReplicatedMap::new("metadata", replica_writer_arc.clone()),
            watermarks: ReplicatedMap::new("watermarks", replica_writer_arc),
        }
    }

    pub fn alias(&self) -> Cache {
        Cache {
            metadata: self.metadata.alias(),
            watermarks: self.watermarks.alias(),
        }
    }
}

impl UpdateReceiver for Cache {
    fn update(&self, name: &str, key_bytes: &[u8], msg: Message) -> Result<()> {
        match name.as_ref() {
            "metadata" => {
                let key = serde_cbor::from_slice::<String>(&key_bytes)
                    .chain_err(|| "Failed to parse key")?;
                match msg.payload() {
                    Some(bytes) => {
                        let metadata = serde_cbor::from_slice::<Metadata>(bytes)
                            .chain_err(|| "Failed to parse payload")?;
                        debug!("Sync metadata cache, key: {}", key);
                        self.metadata.sync_value_update(key, Arc::new(metadata));
                    },
                    None => bail!("Delete not implemented!"),
                };
            },
            _ => {
                bail!("Unknown cache name: {}", name);
            },
        };
        Ok(())
    }
}
// pub struct Cache<K, V>
//   where K: Eq + Hash + Serialize + Deserialize,
//         V: Serialize + Deserialize {
//     cache_lock: Arc<RwLock<HashMap<K, V>>>,
//     on_insert: Option<Box<Fn(&K, &V)>>,
//     on_delete: Option<Box<Fn(&K, &V)>>
// }
//
// impl<K, V> Cache<K, V>
//   where K: Eq + Hash + Serialize + Deserialize,
//         V: Serialize + Deserialize {
//
//     pub fn new() -> Cache<K, V> {
//         Cache {
//             cache_lock: Arc::new(RwLock::new(HashMap::new())),
//             on_insert: None,
//             on_delete: None,
//         }
//     }
//
//     pub fn set_on_insert<'a, CB: 'static + Fn(&K, &V)>(&'a mut self, cb: CB) -> &'a mut Cache<K, V> {
//         self.on_insert = Some(Box::new(cb));
//         self
//     }
//
//     pub fn insert(&self, key: K, value: V) {
//         self.on_insert.as_ref().map(|f| (f)(&key, &value));
//         match self.cache_lock.write() {
//             Ok(mut cache_ref) => (*cache_ref).insert(key, value),
//             Err(_) => panic!("Poison error!"),
//         };
//     }
// }
