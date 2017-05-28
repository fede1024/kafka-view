use rdkafka::{Context, Message};
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::statistics::Statistics;
use futures::{Future, Stream};
use futures::stream::Wait;
use rocket::State;

use cache::Cache;
use config::{ClusterConfig, Config};
use metadata::ClusterId;
use error::*;
use scheduled_executor::{Handle, TaskGroup};

use std::cmp::min;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

//lazy_static! {
//    static ref LIVE_CONSUMERS: LiveConsumerStore = LiveConsumerStore::new();
//}

struct LiveConsumer {
    id: i32,
    consumer: BaseConsumer<EmptyConsumerContext>,
    last_poll: Instant,
}

impl LiveConsumer {
    fn new(id: i32, cluster_config: &ClusterConfig, topic_name: &str) -> Result<LiveConsumer> {
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

        consumer.subscribe(&vec![topic_name])
            .expect("Can't subscribe to specified topics");

        let live_consumer = LiveConsumer {
            id: id,
            consumer: consumer,
            last_poll: Instant::now(),
        };

        info!("Creating new consumer!!");

        Ok(live_consumer)
    }

    fn poll(&self, max_msg: usize, timeout: Duration) -> Vec<Message> {
        let start_time = Instant::now();
        let mut buffer = Vec::new();

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


pub struct LiveConsumerStore {
    consumers: Arc<RwLock<HashMap<i32, Arc<LiveConsumer>>>>
}

impl LiveConsumerStore {
    pub fn new() -> LiveConsumerStore {
        LiveConsumerStore {
            consumers: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    fn get_consumer(&self, id: i32) -> Option<Arc<LiveConsumer>> {
        let consumers = self.consumers.read().expect("Poison error");
        (*consumers).get(&id).cloned()
    }

    fn add_consumer(&self, id: i32, cluster_config: &ClusterConfig, topic_name: &str) -> Result<Arc<LiveConsumer>> {
        let live_consumer = LiveConsumer::new(id, cluster_config, topic_name)
            .chain_err(|| "Failed to add live consumer")?;

        let consumer_arc = Arc::new(live_consumer);

        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(id, Arc::clone(&consumer_arc)),
            Err(_) => panic!("Poison error while writing consumer to cache")
        };

        Ok(consumer_arc)
    }

}

#[get("/api/test/<cluster_id>/<topic>/<id>")]
pub fn test_live_consumer_api(
    cluster_id: ClusterId,
    topic: &str,
    id: i32,
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
    for message in consumer.poll(100, Duration::from_secs(1)) {
        let payload = match message.payload_view::<str>() {
            None => "",
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                warn!("Error while deserializing message payload: {:?}", e);
                ""
            },
        };
        output.push(json!{(message.partition(), message.offset(), payload)});
    }

    Ok(json!(output).to_string())
}

