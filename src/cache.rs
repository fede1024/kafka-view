use serde::ser::Serialize;
use serde::de::Deserialize;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

pub struct Cache<K, V>
  where K: Eq + Hash + Serialize + Deserialize,
        V: Serialize + Deserialize {
    cache_lock: Arc<RwLock<HashMap<K, V>>>,
    on_insert: Option<Box<Fn(&K, &V)>>,
    on_delete: Option<Box<Fn(&K, &V)>>
}

impl<K, V> Cache<K, V>
  where K: Eq + Hash + Serialize + Deserialize,
        V: Serialize + Deserialize {

    pub fn new() -> Cache<K, V> {
        Cache {
            cache_lock: Arc::new(RwLock::new(HashMap::new())),
            on_insert: None,
            on_delete: None,
        }
    }

    pub fn set_on_insert<'a, CB: 'static + Fn(&K, &V)>(&'a mut self, cb: CB) -> &'a mut Cache<K, V> {
        self.on_insert = Some(Box::new(cb));
        self
    }

    pub fn insert(&self, key: K, value: V) {
        self.on_insert.as_ref().map(|f| (f)(&key, &value));
        match self.cache_lock.write() {
            Ok(mut cache_ref) => (*cache_ref).insert(key, value),
            Err(_) => panic!("Poison error!"),
        };
    }
}
