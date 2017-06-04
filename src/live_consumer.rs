use rdkafka::{Context, Message};
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::statistics::Statistics;
use futures::{Future, Stream};
use futures::stream::Wait;
use rocket::State;
use scheduled_executor::CoreExecutor;

use cache::Cache;
use config::{ClusterConfig, Config};
use metadata::ClusterId;
use error::*;
use scheduled_executor::{Handle, TaskGroup};

use std::cmp::min;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::thread;



struct LiveConsumer {
    id: u64,
    cluster_id: ClusterId,
    topic: String,
    last_poll: RwLock<Instant>,
    consumer: Mutex<BaseConsumer<EmptyConsumerContext>>,
    active: AtomicBool,
}

impl LiveConsumer {
    fn new(id: u64, cluster_config: &ClusterConfig, topic: &str) -> Result<LiveConsumer> {
        let mut consumer = ClientConfig::new()
            .set("bootstrap.servers", &cluster_config.bootstrap_servers())
            .set("group.id", &format!("kafka_view_live_consumer_{}", id))
            .set("enable.partition.eof", "false")
            .set("api.version.request", "false")
            .set("enable.auto.commit", "false")
            .set("fetch.message.max.bytes", "102400") // Reduce memory usage
            .create::<BaseConsumer<_>>()
            .chain_err(|| "Failed to create rdkafka consumer")?;

        debug!("Creating live consumer for {}", topic);

        Ok(LiveConsumer {
            id: id,
            cluster_id: cluster_config.cluster_id.clone().unwrap(),
            consumer: Mutex::new(consumer),
            active: AtomicBool::new(false),
            last_poll: RwLock::new(Instant::now()),
            topic: topic.to_owned(),
        })
    }

    fn activate(&self) -> Result<()> {
        // TODO: start from the past
        debug!("Activating live consumer for {}", self.topic);

        let consumer = self.consumer.lock().unwrap();
        consumer.subscribe(vec![self.topic.as_str()].as_slice())
            .chain_err(|| "Can't subscribe to specified topics")?;
        self.active.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn last_poll(&self) -> Instant {
        *self.last_poll.read().unwrap()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    fn poll(&self, max_msg: usize, timeout: Duration) -> Vec<Message> {
        let start_time = Instant::now();
        let mut buffer = Vec::new();
        *self.last_poll.write().unwrap() = Instant::now();
        let mut consumer = self.consumer.lock().unwrap();

        while Instant::elapsed(&start_time) < timeout && buffer.len() < max_msg {
            match (*consumer).poll(100) {
                Ok(None) => {},
                Ok(Some(m)) => buffer.push(m),
                Err(e) => {
                    error!("Error while receiving message {:?}", e);
                },
            };
        }

        debug!("{} messages received in {:?}", buffer.len(), Instant::elapsed(&start_time));
        buffer
    }
}

impl Drop for LiveConsumer {
    fn drop(&mut self) {
        debug!("Dropping consumer {}", self.id);
    }
}

type LiveConsumerMap = HashMap<u64, Arc<LiveConsumer>>;

fn remove_idle_consumers(consumers: &mut LiveConsumerMap) {
    consumers.retain(|_, ref consumer| consumer.last_poll().elapsed() < Duration::from_secs(20));
}

pub struct LiveConsumerStore {
    consumers: Arc<RwLock<LiveConsumerMap>>,
    executor: CoreExecutor,
}

impl LiveConsumerStore {
    pub fn new(executor: CoreExecutor) -> LiveConsumerStore {
        let consumers = Arc::new(RwLock::new(HashMap::new()));
        let consumers_clone = Arc::clone(&consumers);
        executor.schedule_fixed_interval(
            Duration::from_secs(10),
            move |_handle| {
                let mut consumers = consumers_clone.write().unwrap();
                remove_idle_consumers(&mut *consumers);
            }
        );
        LiveConsumerStore {
            consumers: consumers,
            executor: executor,
        }
    }

    pub fn get_consumer(&self, id: u64) -> Option<Arc<LiveConsumer>> {
        let consumers = self.consumers.read().expect("Poison error");
        (*consumers).get(&id).cloned()
    }

    pub fn add_consumer(&self, id: u64, cluster_config: &ClusterConfig, topic: &str) -> Result<Arc<LiveConsumer>> {
        let live_consumer = LiveConsumer::new(id, cluster_config, topic)
            .chain_err(|| "Failed to create live consumer")?;

        let live_consumer_arc = Arc::new(live_consumer);

        // Add consumer immediately to the store, to prevent other threads from adding it again.
        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(id, live_consumer_arc.clone()),
            Err(_) => panic!("Poison error while writing consumer to cache")
        };

        live_consumer_arc.activate()
            .chain_err(|| "Failed to activate live consumer")?;

        Ok(live_consumer_arc)
    }

    pub fn consumers(&self) -> Vec<Arc<LiveConsumer>> {
        self.consumers.read().unwrap().iter()
            .map(|(_, consumer)| consumer.clone())
            .collect::<Vec<_>>()
    }
}


// TODO: check log in case of error

#[get("/api/test/<cluster_id>/<topic>/<id>")]
pub fn test_live_consumer_api(
    cluster_id: ClusterId,
    topic: &str,
    id: u64,
    cache: State<Cache>,
    config: State<Config>,
    live_consumers_store: State<LiveConsumerStore>,
) -> Result<String> {
    let cluster_config = config.clusters.get(&cluster_id);

    if cluster_config.is_none() {
        return Ok("[]".to_owned());
    }
    let cluster_config = cluster_config.unwrap();

    let mut consumer = match live_consumers_store.get_consumer(id) {
        Some(consumer) => consumer,
        None => live_consumers_store.add_consumer(id, cluster_config, topic)
            .chain_err(|| format!("Error while creating live consumer for {} {}", cluster_id, topic))?,
    };

    if !consumer.is_active() {
        // Consumer is still being activated, no results for now.
        return Ok("[]".to_owned());
    }

    let mut output = Vec::new();
    for message in consumer.poll(100, Duration::from_secs(3)) {
        let payload = message.payload()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
            .unwrap_or("".to_owned());
        output.push(json!{(message.partition(), message.offset(), payload)});
    }

    Ok(json!(output).to_string())
}

