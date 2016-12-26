use serde::ser::Serialize;
use serde::de::Deserialize;
use serde_json;
use rdkafka::config::TopicConfig;
use rdkafka::producer::{FutureProducer, EmptyProducerContext, FutureProducerTopic};
use rdkafka::config::ClientConfig;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use typemap::{Key, TypeMap};
use std::marker::PhantomData;
use std::any::{Any, TypeId};

use error::*;

type ReplicatorProducer = FutureProducer<EmptyProducerContext>;
type ReplicatorTopic = FutureProducerTopic<EmptyProducerContext>;

pub struct Replicator {
//    brokers: String,
//    topic_name: String,
    producer_topic: Arc<ReplicatorTopic>,
    type_map: TypeMap,
}

// pub struct MapKey<V: ReplicatedCache + Clone + 'static> {
//     _p: PhantomData<V>
// }
// 
// impl<V: ReplicatedCache + Clone + 'static> Key for MapKey<V> {
//     type Value = V;
// }
// 
// impl<V: ReplicatedCache + Clone + 'static> MapKey<V> {
//     pub fn new() -> MapKey<V> {
//         MapKey { _p: PhantomData }
//     }
// }


struct FakeType {
    id: u64,
}

impl FakeType {
    fn new(id: u64) -> FakeType {
        FakeType { id : id }
    }
}

impl Any for FakeType {
    fn get_type_id(&self) -> TypeId {
        TypeId { t: self.id }
    }
}

impl Replicator {
    pub fn new(brokers: &str, topic_name: &str) -> Result<Replicator> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("compression.codec", "gzip")
            .set("message.max.bytes", "10000000")
            .create::<FutureProducer<_>>()
            .expect("Producer creation error");

        producer.start();

        let topic = producer.get_topic(topic_name, &TopicConfig::new())
            .expect("Topic creation error");

        let replicator = Replicator {
//            brokers: brokers.to_owned(),
//            topic_name: topic_name.to_owned(),
            producer_topic: Arc::new(topic),
            type_map: TypeMap::new(),
        };

        Ok(replicator)
    }

    // pub fn create_cache<C: ReplicatedCache>(&self, name: &str) -> C {
    //     C::new(self.producer_topic.clone(), name)
    // }

    // pub fn create_cache<V: ReplicatedCache + Clone + 'static>(&mut self, km: MapKey<V>) -> V {
    //     let cache = V::new(self.producer_topic.clone(), "LOL");
    //     self.type_map.insert::<MapKey<V>>(cache.clone());
    //     cache
    // }

    pub fn create_cache<K: Key>(&mut self, name: &str) -> K::Value
          where K::Value: ReplicatedCache + Clone {
        let cache = K::Value::new(self.producer_topic.clone(), name);
        self.type_map.insert::<K>(cache.clone());
        println!(">> {:?}", TypeId::of::<K>());
        cache
    }
}

pub trait ReplicatedCache {
    type Key: Serialize + Deserialize;
    type Value: Serialize + Deserialize;

    fn new(Arc<ReplicatorTopic>, &str) -> Self;
    fn name(&self) -> &str;
    fn insert(&self, Self::Key, Self::Value) -> Result<Arc<Self::Value>>;
    fn get(&self, &Self::Key) -> Option<Arc<Self::Value>>;
    fn keys(&self) -> Vec<Self::Key>;
}

#[derive(Serialize, Deserialize)]
struct WrappedKey<K>
  where K: Serialize + Deserialize {
    id: i32,
    map_name: String,
    key: K,
}

impl<K> WrappedKey<K>
  where K: Serialize + Deserialize {
    fn new(id: i32, map_name: String, key: K) -> WrappedKey<K> {
        WrappedKey {
            id: id,
            map_name: map_name,
            key: key,
        }
    }
}

// impl<V> ValueContainer<V>
//   where K: Serialize + Deserialize {
//     fn new(id: i32, value: V) -> WrappedKey<V> {
//         ValueContainer {
//             id: id,
//             value: value,
//         }
//     }
// }

// TODO: add name to key
// TODO: use structure for value
fn write_update<K, V>(topic: &ReplicatorTopic, name: &str, key: &K, value: &V) -> Result<()>
        where K: Serialize + Deserialize + Clone,
              V: Serialize + Deserialize {
    let serialized_key = serde_json::to_vec(&WrappedKey::new(1234, name.to_owned(), key.clone()))
        .chain_err(|| "Failed to serialize key")?;
    let serialized_value = serde_json::to_vec(&value)
        .chain_err(|| "Failed to serialize value")?;
    trace!("Serialized value size: {}", serialized_value.len());
    let _f = topic.send_copy(None, Some(&serialized_value), Some(&serialized_key))
        .chain_err(|| "Failed to produce message")?;
    // _f.wait();  // Uncomment to make production synchronous
    Ok(())
}

// TODO? use inner object with one Arc?
#[derive(Clone)]
pub struct Cache<K, V>
  where K: Eq + Hash + Clone + Serialize + Deserialize,
        V: Serialize + Deserialize {
    name: String,
    cache_lock: Arc<RwLock<HashMap<K, Arc<V>>>>,
    replicator_topic: Arc<ReplicatorTopic>,
}

impl<K, V> ReplicatedCache for Cache<K, V>
    where K: Eq + Hash + Clone + Serialize + Deserialize,
          V: Serialize + Deserialize {
    type Key = K;
    type Value = V;

    fn new(replicator_topic: Arc<ReplicatorTopic>, name: &str) -> Cache<K, V> {
        Cache {
            name: name.to_owned(),
            cache_lock: Arc::new(RwLock::new(HashMap::new())),
            replicator_topic: replicator_topic,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn keys(&self) -> Vec<K> {
        match self.cache_lock.read() {
            Ok(ref cache) => (*cache).keys().cloned().collect::<Vec<_>>(),
            Err(_) => panic!("Poison error"),
        }
    }

    fn insert(&self, key: K, value: V) -> Result<Arc<V>> {
        write_update(self.replicator_topic.as_ref(), &self.name, &key, &value)
            .chain_err(|| "Failed to write cache update")?;
        let value_arc = Arc::new(value);
        match self.cache_lock.write() {
            Ok(mut cache) => (*cache).insert(key, value_arc.clone()),
            Err(_) => panic!("Poison error"),
        };
        Ok(value_arc.clone())
    }

    fn get(&self, key: &K) -> Option<Arc<V>> {
        let value = match self.cache_lock.read() {
            Ok(cache) => (*cache).get(key).map(|arc| arc.clone()),
            Err(_) => panic!("Poison error"),
        };
        value
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
