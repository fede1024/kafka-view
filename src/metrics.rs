use curl::easy::Easy;
use serde_json::Value;
use chrono::{DateTime, UTC};
use serde_json;
use regex::Regex;
use scheduled_executor::{Handle, TaskGroup};

use std::time::Duration;
use std::collections::HashMap;

use cache::{Cache, MetricsCache};
use error::*;
use metadata::{ClusterId, BrokerId, Broker, TopicName};


fn format_jolokia_path(hostname: &str, port: i32, filter: &str) -> String {
    format!("http://{}:{}/jolokia/read/{}?ignoreErrors=true&includeStackTrace=false&maxCollectionSize=0",
            hostname, port, filter)
}

fn fetch_metrics_json(hostname: &str, port: i32, filter: &str) -> Result<Value> {
    let mut req = Easy::new();
    let url = format_jolokia_path(hostname, port, filter);
    req.url(&url).chain_err(|| format!("Unable to parse url: '{}'", url))?;

    let mut buf = Vec::new();
    {
        let mut transfer = req.transfer();
        transfer.write_function(|data| {
            buf.extend_from_slice(data);
            Ok(data.len())
        }).chain_err(|| "Data transfer failure")?;
        transfer.perform().chain_err(|| "Connection failure")?;
    }
    let string = String::from_utf8(buf)
        .chain_err(|| "Failed to parse buffer as UTF-8")?;
    let value = serde_json::from_str(&string).chain_err(|| "Failed to parse JSON")?;
    Ok(value)
}

fn jolokia_response_get_value(json_response: &Value) -> Result<&serde_json::Map<String, Value>> {
    let obj = match json_response.as_object() {
        Some(obj) => obj,
        None => bail!("The provided Value is not a JSON object"),
    };
    if let Some(v) = obj.get("value") {
        if let Some(value_obj) = v.as_object() {
            return Ok(value_obj);
        } else {
            bail!("'value' is not a JSON object");
        }
    } else {
        bail!("Missing value");
    }
}

fn parse_broker_rate_metrics(jolokia_json_response: &Value) -> Result<HashMap<TopicName, f64>> {
    let value_map = jolokia_response_get_value(jolokia_json_response)
        .chain_err(|| "Failed to extract 'value' from jolokia response.")?;
    let mut metrics = HashMap::new();
    let re = Regex::new(r"topic=([^,]+),").unwrap();

    for (mbean_name, value) in value_map.iter() {
        let topic = match re.captures(mbean_name) {
            Some(cap) => cap.at(1).unwrap(),
            None => "__TOTAL__",
        };
        match *value {
            Value::Object(ref obj) => {
                match obj.get("FifteenMinuteRate") {
                    Some(&Value::Number(ref rate)) => metrics.insert(topic.to_owned(), rate.as_f64().unwrap_or(0f64)),
                    None => bail!("Can't find key in metric"),
                    _ => bail!("Unexpected metric type"),
                };
            },
            _ => {},
        }
    }
    Ok(metrics)
}

fn log_elapsed_time(task_name: &str, start: DateTime<UTC>) {
    debug!("{} completed in: {:.3}ms", task_name, UTC::now().signed_duration_since(start).num_microseconds().unwrap() as f64 / 1000f64);
}

// TODO: make faster?
pub fn build_topic_metrics(cluster_id: &ClusterId, brokers: &Vec<Broker>, topic_count: usize,
                           metrics: &MetricsCache) -> HashMap<TopicName, (f64, f64)> {
    let mut result = HashMap::with_capacity(topic_count);
    for broker in brokers.iter() {
        if let Some(broker_metrics) = metrics.get(&(cluster_id.clone(), broker.id)) {
            for (topic_name, rate) in broker_metrics.topics {
                // Keep an eye on RFC 1769
                let mut entry_ref = result.entry(topic_name.to_owned()).or_insert((0f64, 0f64));
                *entry_ref = (entry_ref.0 + rate.0, entry_ref.1 + rate.1);
            }
        }
    }
    result
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMetrics {
    pub topics: HashMap<TopicName, (f64, f64)>,
}

impl BrokerMetrics {
    fn new() -> BrokerMetrics {
        BrokerMetrics { topics: HashMap::new() }
    }

    fn set(&mut self, topic: &str, fifteen_minute_byte_rate: f64, fifteen_minute_msg_rate: f64) {
        self.topics.insert(topic.to_owned(), (fifteen_minute_byte_rate, fifteen_minute_msg_rate));
    }
}

pub struct MetricsFetchTaskGroup {
    cache: Cache,
}

impl MetricsFetchTaskGroup {
    pub fn new(cache: &Cache) -> MetricsFetchTaskGroup {
        MetricsFetchTaskGroup {
            cache: cache.alias(),
        }
    }

    fn fetch_metrics(&self, cluster_id: &ClusterId, broker: &Broker) -> Result<()> {
        let start = UTC::now();
        let byte_rate_json = fetch_metrics_json(&broker.hostname, 8778, "kafka.server:name=BytesInPerSec,*,type=BrokerTopicMetrics/FifteenMinuteRate")
            .chain_err(|| format!("Failed to fetch byte rate metrics from {}", broker.hostname))?;
        let byte_rate_metrics = parse_broker_rate_metrics(&byte_rate_json)
            .chain_err(|| "Failed to parse byte rate broker metrics")?;
        let msg_rate_json = fetch_metrics_json(&broker.hostname, 8778, "kafka.server:name=MessagesInPerSec,*,type=BrokerTopicMetrics/FifteenMinuteRate")
            .chain_err(|| format!("Failed to fetch message rate metrics from {}", broker.hostname))?;
        let msg_rate_metrics = parse_broker_rate_metrics(&msg_rate_json)
            .chain_err(|| "Failed to parse message rate broker metrics")?;
        let mut metrics = BrokerMetrics::new();
        for (topic, byte_rate) in byte_rate_metrics {
            let msg_rate = msg_rate_metrics.get(&topic).unwrap_or(&-1f64).clone();
            metrics.set(&topic, byte_rate, msg_rate);
        }
        self.cache.metrics.insert((cluster_id.clone(), broker.id), metrics)
            .chain_err(|| "Failed to update metrics cache")?;
        log_elapsed_time("metrics fetch", start);
        Ok(())
    }
}

impl TaskGroup for MetricsFetchTaskGroup {
    type TaskId = (ClusterId, Broker);

    fn get_tasks(&self) -> Vec<(ClusterId, Broker)> {
        self.cache.brokers.lock_iter(|iter| {
            let mut tasks = Vec::new();
            for (cluster_id, brokers) in iter {
                for broker in brokers {
                    tasks.push((cluster_id.clone(), broker.clone()));
                }
            }
            tasks
        })
    }

    fn execute(&self, task_id: (ClusterId, Broker), _: Option<Handle>) {
        debug!("Starting fetch for {}: {}", task_id.0, task_id.1.id);
        if let Err(e) = self.fetch_metrics(&task_id.0, &task_id.1) {
            format_error_chain!(e);
        }
    }
}

