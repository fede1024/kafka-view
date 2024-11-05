use chrono::{DateTime, Utc};
use hyper::Client;
use regex::Regex;
use scheduled_executor::TaskGroup;
use serde_json;
use serde_json::Value;

use std::collections::{HashMap, HashSet};
use std::f64;
use std::io::Read;

use crate::cache::Cache;
use crate::config::Config;
use crate::error::*;
use crate::metadata::{Broker, ClusterId, TopicName};
use crate::utils::insert_at;

#[derive(PartialEq, Serialize, Deserialize, Debug, Copy, Clone)]
pub struct PartitionMetrics {
    pub size_bytes: f64,
}

impl Default for PartitionMetrics {
    fn default() -> PartitionMetrics {
        PartitionMetrics { size_bytes: 0f64 }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct TopicBrokerMetrics {
    pub m_rate_15: f64,
    pub b_rate_15: f64,
    pub partitions: Vec<PartitionMetrics>,
}

impl Default for TopicBrokerMetrics {
    fn default() -> Self {
        TopicBrokerMetrics {
            m_rate_15: 0f64,
            b_rate_15: 0f64,
            partitions: Vec::new(),
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct TopicMetrics {
    pub brokers: HashMap<i32, TopicBrokerMetrics>,
}

impl TopicMetrics {
    pub fn new() -> TopicMetrics {
        TopicMetrics {
            brokers: HashMap::new(),
        }
    }

    pub fn aggregate_broker_metrics(&self) -> TopicBrokerMetrics {
        self.brokers.iter().fold(
            TopicBrokerMetrics::default(),
            |mut acc, (_, broker_metrics)| {
                acc.m_rate_15 += broker_metrics.m_rate_15;
                acc.b_rate_15 += broker_metrics.b_rate_15;
                acc
            },
        )
    }
}

impl Default for TopicMetrics {
    fn default() -> Self {
        TopicMetrics::new()
    }
}

fn format_jolokia_path(hostname: &str, port: i32, filter: &str) -> String {
    format!("http://{}:{}/jolokia/read/{}?ignoreErrors=true&includeStackTrace=false&maxCollectionSize=0",
            hostname, port, filter)
}

fn fetch_metrics_json(hostname: &str, port: i32, filter: &str) -> Result<Value> {
    let client = Client::new();
    let url = format_jolokia_path(hostname, port, filter);
    let mut response = client.get(&url).send().chain_err(|| "Connection error")?;

    let mut body = String::new();
    response
        .read_to_string(&mut body)
        .chain_err(|| "Could not read response to string")?;

    let value = serde_json::from_str(&body).chain_err(|| "Failed to parse JSON")?;

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
            Some(cap) => cap.get(1).unwrap().as_str(),
            None => "__TOTAL__",
        };
        if let Value::Object(ref obj) = *value {
            match obj.get("FifteenMinuteRate") {
                Some(&Value::Number(ref rate)) => {
                    metrics.insert(topic.to_owned(), rate.as_f64().unwrap_or(-1f64))
                }
                None => bail!("Can't find key in metric"),
                _ => bail!("Unexpected metric type"),
            };
        }
    }
    Ok(metrics)
}

fn parse_partition_size_metrics(
    jolokia_json_response: &Value,
) -> Result<HashMap<TopicName, Vec<PartitionMetrics>>> {
    let value_map = jolokia_response_get_value(jolokia_json_response)
        .chain_err(|| "Failed to extract 'value' from jolokia response.")?;
    let topic_re = Regex::new(r"topic=([^,]+),").unwrap();
    let partition_re = Regex::new(r"partition=([^,]+),").unwrap();

    let mut metrics = HashMap::new();
    for (mbean_name, value) in value_map.iter() {
        let topic = topic_re
            .captures(mbean_name)
            .and_then(|cap| cap.get(1).map(|m| m.as_str()));
        let partition = partition_re
            .captures(mbean_name)
            .and_then(|cap| cap.get(1).map(|m| m.as_str()))
            .and_then(|p_str| p_str.parse::<u32>().ok());
        if topic.is_none() || partition.is_none() {
            bail!("Can't parse topic and partition metadata from metric name");
        }
        if let Value::Object(ref obj) = *value {
            match obj.get("Value") {
                Some(&Value::Number(ref size)) => {
                    let partition_metrics = PartitionMetrics {
                        size_bytes: size.as_f64().unwrap_or(-1f64),
                    };
                    insert_at(
                        metrics
                            .entry(topic.unwrap().to_owned())
                            .or_insert_with(Vec::new),
                        partition.unwrap() as usize,
                        partition_metrics,
                        PartitionMetrics::default(),
                    );
                }
                None => bail!("Can't find key in metric"),
                _ => bail!("Unexpected metric type"),
            };
        }
    }
    Ok(metrics)
}

fn log_elapsed_time(task_name: &str, start: DateTime<Utc>) {
    debug!(
        "{} completed in: {:.3}ms",
        task_name,
        Utc::now()
            .signed_duration_since(start)
            .num_microseconds()
            .unwrap() as f64
            / 1000f64
    );
}

pub struct MetricsFetchTaskGroup {
    cache: Cache,
    config: Config,
}

impl MetricsFetchTaskGroup {
    pub fn new(cache: &Cache, config: &Config) -> MetricsFetchTaskGroup {
        MetricsFetchTaskGroup {
            cache: cache.alias(),
            config: config.clone(),
        }
    }

    fn fetch_metrics(&self, cluster_id: &ClusterId, broker: &Broker, port: i32) -> Result<()> {
        let start = Utc::now();
        let byte_rate_json = fetch_metrics_json(
            &broker.hostname,
            port,
            "kafka.server:name=BytesInPerSec,*,type=BrokerTopicMetrics/FifteenMinuteRate",
        )
        .chain_err(|| format!("Failed to fetch byte rate metrics from {}", broker.hostname))?;

        let byte_rate_metrics = parse_broker_rate_metrics(&byte_rate_json)
            .chain_err(|| "Failed to parse byte rate broker metrics")?;

        let msg_rate_json = fetch_metrics_json(
            &broker.hostname,
            port,
            "kafka.server:name=MessagesInPerSec,*,type=BrokerTopicMetrics/FifteenMinuteRate",
        )
        .chain_err(|| {
            format!(
                "Failed to fetch message rate metrics from {}",
                broker.hostname
            )
        })?;

        let msg_rate_metrics = parse_broker_rate_metrics(&msg_rate_json)
            .chain_err(|| "Failed to parse message rate broker metrics")?;
        let partition_metrics_json = fetch_metrics_json(
            &broker.hostname,
            port,
            "kafka.log:name=Size,*,type=Log/Value",
        )
        .chain_err(|| {
            format!(
                "Failed to fetch partition size metrics from {}",
                broker.hostname
            )
        })?;

        let pt_size_metrics = parse_partition_size_metrics(&partition_metrics_json)
            .chain_err(|| "Failed to parse partition size broker metrics")?;

        let topics = byte_rate_metrics
            .keys()
            .chain(msg_rate_metrics.keys())
            .chain(pt_size_metrics.keys())
            .collect::<HashSet<_>>();

        for topic in topics {
            let mut topic_metrics = self
                .cache
                .metrics
                .get(&(cluster_id.clone(), topic.clone()))
                .unwrap_or_default();

            let b_rate_15 = *byte_rate_metrics.get(topic).unwrap_or(&-1f64);
            let m_rate_15 = *msg_rate_metrics.get(topic).unwrap_or(&-1f64);
            let partitions = pt_size_metrics.get(topic).cloned().unwrap_or_else(Vec::new);
            topic_metrics.brokers.insert(
                broker.id,
                TopicBrokerMetrics {
                    m_rate_15,
                    b_rate_15,
                    partitions,
                },
            );

            self.cache
                .metrics
                .insert((cluster_id.clone(), topic.clone()), topic_metrics)
                .chain_err(|| "Failed to insert to metrics")?;
        }
        log_elapsed_time("metrics fetch", start);
        Ok(())
    }
}

impl TaskGroup for MetricsFetchTaskGroup {
    type TaskId = (ClusterId, Broker, i32);

    fn get_tasks(&self) -> Vec<Self::TaskId> {
        self.cache.brokers.lock_iter(|iter| {
            let mut tasks = Vec::new();
            for (cluster_id, brokers) in iter {
                let port = self
                    .config
                    .cluster(cluster_id)
                    .and_then(|cluster_config| cluster_config.jolokia_port);
                if port.is_some() {
                    for broker in brokers {
                        tasks.push((cluster_id.clone(), broker.clone(), port.unwrap()));
                    }
                }
            }
            debug!("New metrics tasks: {:?}", tasks);
            tasks
        })
    }

    fn execute(&self, task_id: (ClusterId, Broker, i32)) {
        debug!("Starting fetch for {}: {}", task_id.0, task_id.1.id);
        if let Err(e) = self.fetch_metrics(&task_id.0, &task_id.1, task_id.2) {
            format_error_chain!(e);
        }
    }
}
