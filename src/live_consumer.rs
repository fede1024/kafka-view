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
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::thread;



struct LiveConsumerInner {
    id: u64,
    consumer: BaseConsumer<EmptyConsumerContext>,
    last_poll: Instant,
}

impl LiveConsumerInner {
    fn new(id: u64, cluster_config: &ClusterConfig, topic_name: &str) -> Result<LiveConsumerInner> {
        let mut consumer = ClientConfig::new()
            .set("bootstrap.servers", &cluster_config.bootstrap_servers())
            .set("group.id", &format!("kafka_view_live_consumer_{}", id))
            .set("enable.partition.eof", "false")
            .set("api.version.request", "false")
            .set("queued.min.messages", "5000")
            .set("enable.auto.commit", "false")
            .set("queued.max.messages.kbytes", "1000")
            .set("fetch.message.max.bytes", "102400")
            .create::<BaseConsumer<_>>()
            .chain_err(|| "Failed to create rdkafka consumer")?;

        // TODO: start from the past
        consumer.subscribe(&vec![topic_name])
            .expect("Can't subscribe to specified topics");

        Ok(LiveConsumerInner {
            id: id,
            consumer: consumer,
            last_poll: Instant::now(),
        })
    }

    fn poll(&mut self, max_msg: usize, timeout: Duration) -> Vec<Message> {
        let start_time = Instant::now();
        let mut buffer = Vec::new();
        self.last_poll = Instant::now();

        while Instant::elapsed(&start_time) < timeout && buffer.len() < max_msg {
            match self.consumer.poll(100) {
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

impl Drop for LiveConsumerInner {
    fn drop(&mut self) {
        debug!("Dropping consumer {}", self.id);
    }
}

struct LiveConsumer {
    id: u64,
    inner: Arc<RwLock<LiveConsumerInner>>
}

impl Clone for LiveConsumer {
    fn clone(&self) -> Self {
        LiveConsumer {
            id: self.id,
            inner: Arc::clone(&self.inner)
        }
    }
}

impl LiveConsumer {
    fn new(id: u64, cluster_config: &ClusterConfig, topic_name: &str) -> Result<LiveConsumer> {
        let inner = LiveConsumerInner::new(id, cluster_config, topic_name)
            .chain_err(|| "Error while creating LiveConsumerInner")?;

        let live_consumer = LiveConsumer {
            id: id,
            inner: Arc::new(RwLock::new(inner)),
        };

        Ok(live_consumer)
    }

    fn poll(&self, max_msg: usize, timeout: Duration) -> Vec<Message> {
        let mut inner = self.inner.write().unwrap();
        (*inner).poll(max_msg, timeout)
    }

    fn last_poll_time(&self) -> Instant {
        let inner = self.inner.read().unwrap();
        (*inner).last_poll
    }
}


type LiveConsumerMap = HashMap<u64, LiveConsumer>;

fn remove_idle_consumers(consumers: &mut LiveConsumerMap) {
    consumers.retain(|_, ref consumer| consumer.last_poll_time().elapsed() < Duration::from_secs(20));
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

    fn get_consumer(&self, id: u64) -> Option<LiveConsumer> {
        let consumers = self.consumers.read().expect("Poison error");
        (*consumers).get(&id).cloned()
    }

    fn add_consumer(&self, id: u64, cluster_config: &ClusterConfig, topic_name: &str) -> Result<LiveConsumer> {
        let live_consumer = LiveConsumer::new(id, cluster_config, topic_name)
            .chain_err(|| "Failed to add live consumer")?;

        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(id, live_consumer.clone()),
            Err(_) => panic!("Poison error while writing consumer to cache")
        };

        Ok(live_consumer)
    }

}

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
        return Ok("".to_owned());
    }
    let cluster_config = cluster_config.unwrap();

    let mut consumer = match live_consumers_store.get_consumer(id) {
        Some(consumer) => consumer,
        None => live_consumers_store.add_consumer(id, cluster_config, topic)
            .chain_err(|| format!("Error while creating live consumer for {} {}", cluster_id, topic))?,
    };

    let mut output = Vec::new();
    for message in consumer.poll(100, Duration::from_secs(3)) {
        let payload = message.payload()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
            .unwrap_or("".to_owned());
        output.push(json!{(message.partition(), message.offset(), payload)});
    }

    Ok(json!(output).to_string())
}

