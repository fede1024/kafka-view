use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Timestamp::*;
use rdkafka::Message;
use rocket::State;
use scheduled_executor::ThreadPoolExecutor;

use crate::config::{ClusterConfig, Config};
use crate::error::*;
use crate::metadata::ClusterId;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub struct LiveConsumer {
    id: u64,
    cluster_id: ClusterId,
    topic: String,
    last_poll: RwLock<Instant>,
    consumer: BaseConsumer<DefaultConsumerContext>,
    active: AtomicBool,
}

impl LiveConsumer {
    fn new(id: u64, cluster_config: &ClusterConfig, topic: &str) -> Result<LiveConsumer> {
        info!("security_protocol={}",&cluster_config.security_protocol.clone().unwrap_or("".to_owned()) );
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &cluster_config.bootstrap_servers())
            .set("security.protocol", &cluster_config.security_protocol.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.service.name", &cluster_config.sasl_kerberos_service_name.clone().unwrap_or("".to_owned()) )
            .set("sasl.mechanism", &cluster_config.sasl_mechanism.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.principal", &cluster_config.sasl_kerberos_principal.clone().unwrap_or("".to_owned()) )
            .set("sasl.kerberos.keytab", &cluster_config.sasl_kerberos_keytab.clone().unwrap_or("".to_owned()) )
            .set("group.id", &format!("kafka_view_live_consumer_{}", id))
            .set("enable.partition.eof", "false")
            .set("api.version.request", "true")
            .set("enable.auto.commit", "false")
            .set("queued.max.messages.kbytes", "100") // Reduce memory usage
            .set("fetch.message.max.bytes", "102400")
            //.set("debug", "all")
            .create::<BaseConsumer<_>>()
            .chain_err(|| "Failed to create rdkafka consumer")?;

        debug!("Creating live consumer for {}", topic);

        Ok(LiveConsumer {
            id,
            cluster_id: cluster_config.cluster_id.clone().unwrap(),
            consumer,
            active: AtomicBool::new(false),
            last_poll: RwLock::new(Instant::now()),
            topic: topic.to_owned(),
        })
    }

    fn activate(&self) -> Result<()> {
        // TODO: start from the past
        debug!("Activating live consumer for {}", self.topic);

        // TODO: use assign instead
        self.consumer
            .subscribe(vec![self.topic.as_str()].as_slice())
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

    fn poll(&self, max_msg: usize, timeout: Duration) -> Vec<BorrowedMessage> {
        let start_time = Instant::now();
        let mut buffer = Vec::new();
        *self.last_poll.write().unwrap() = Instant::now();

        while Instant::elapsed(&start_time) < timeout && buffer.len() < max_msg {
            match self.consumer.poll(rdkafka::util::Timeout::After(Duration::from_secs(100))) {
                None => {}
                Some(Ok(m)) => buffer.push(m),
                Some(Err(e)) => {
                    error!("Error while receiving message {:?}", e);
                }
            };
        }

        debug!(
            "{} messages received in {:?}",
            buffer.len(),
            Instant::elapsed(&start_time)
        );
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
    _executor: ThreadPoolExecutor,
}

impl LiveConsumerStore {
    pub fn new(executor: ThreadPoolExecutor) -> LiveConsumerStore {
        let consumers = Arc::new(RwLock::new(HashMap::new()));
        let consumers_clone = Arc::clone(&consumers);
        executor.schedule_fixed_rate(
            Duration::from_secs(10),
            Duration::from_secs(10),
            move |_handle| {
                let mut consumers = consumers_clone.write().unwrap();
                remove_idle_consumers(&mut *consumers);
            },
        );
        LiveConsumerStore {
            consumers,
            _executor: executor,
        }
    }

    fn get_consumer(&self, id: u64) -> Option<Arc<LiveConsumer>> {
        let consumers = self.consumers.read().expect("Poison error");
        (*consumers).get(&id).cloned()
    }

    fn add_consumer(
        &self,
        id: u64,
        cluster_config: &ClusterConfig,
        topic: &str,
    ) -> Result<Arc<LiveConsumer>> {
        let live_consumer = LiveConsumer::new(id, cluster_config, topic)
            .chain_err(|| "Failed to create live consumer")?;

        let live_consumer_arc = Arc::new(live_consumer);

        // Add consumer immediately to the store, to prevent other threads from adding it again.
        match self.consumers.write() {
            Ok(mut consumers) => (*consumers).insert(id, live_consumer_arc.clone()),
            Err(_) => panic!("Poison error while writing consumer to cache"),
        };

        live_consumer_arc
            .activate()
            .chain_err(|| "Failed to activate live consumer")?;

        Ok(live_consumer_arc)
    }

    pub fn consumers(&self) -> Vec<Arc<LiveConsumer>> {
        self.consumers
            .read()
            .unwrap()
            .iter()
            .map(|(_, consumer)| consumer.clone())
            .collect::<Vec<_>>()
    }
}

// TODO: check log in case of error

#[derive(Serialize)]
struct TailedMessage {
    partition: i32,
    offset: i64,
    key: Option<String>,
    created_at: Option<i64>,
    appended_at: Option<i64>,
    payload: String,
}

#[get("/api/tailer/<cluster_id>/<topic>/<id>")]
pub fn topic_tailer_api(
    cluster_id: ClusterId,
    topic: String,
    id: u64,
    config: &State<Config>,
    live_consumers_store: &State<LiveConsumerStore>,
) -> std::result::Result<String,String> {
    let cluster_config = config.clusters.get(&cluster_id);

    if cluster_config.is_none() || !cluster_config.unwrap().enable_tailing {
        return Ok("[]".to_owned());
    }
    let cluster_config = cluster_config.unwrap();

    let consumer = match live_consumers_store.get_consumer(id) {
        Some(consumer) => consumer,
        None => live_consumers_store
            .add_consumer(id, cluster_config, &topic)
            .chain_err(|| {
                format!(
                    "Error while creating live consumer for {} {}",
                    cluster_id, topic
                )
            }).expect("Failed to create live consumer"),
    };

    if !consumer.is_active() {
        // Consumer is still being activated, no results for now.
        return Ok("[]".to_owned());
    }

    let mut output = Vec::new();
    for message in consumer.poll(100, Duration::from_secs(3)) {
        let key = message
            .key()
            .map(|bytes| String::from_utf8_lossy(bytes))
            .map(|cow_str| cow_str.into_owned());

        let mut created_at = None;
        let mut appended_at = None;
        match message.timestamp() {
            CreateTime(ctime) => created_at = Some(ctime),
            LogAppendTime(atime) => appended_at = Some(atime),
            NotAvailable => (),
        }

        let original_payload = message
            .payload()
            .map(|bytes| String::from_utf8_lossy(bytes))
            .unwrap_or(Cow::Borrowed(""));
        let payload = if original_payload.len() > 1024 {
            format!(
                "{}...",
                original_payload.chars().take(1024).collect::<String>()
            )
        } else {
            original_payload.into_owned()
        };

        output.push(TailedMessage {
            partition: message.partition(),
            offset: message.offset(),
            key,
            created_at,
            appended_at,
            payload,
        })
    }

    Ok(json!(output).to_string())
}
