use futures::{future, Future};
use futures_cpupool::Builder;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaResult;
use regex::Regex;
use rocket::http::RawStr;
use rocket::State;

use crate::cache::Cache;
use crate::config::Config;
use crate::error::*;
use crate::live_consumer::LiveConsumerStore;
use crate::metadata::{ClusterId, TopicName, TopicPartition, CONSUMERS};
use crate::offsets::OffsetStore;
use crate::web_server::pages::omnisearch::OmnisearchFormParams;
use crate::zk::ZK;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

//
// ********** TOPICS LIST **********
//

#[derive(Serialize)]
struct TopicDetails {
    topic_name: String,
    partition_count: usize,
    errors: String,
    b_rate_15: f64,
    m_rate_15: f64,
}

#[get("/api/clusters/<cluster_id>/topics")]
pub fn cluster_topics(cluster_id: ClusterId, cache: &State<Cache>) -> String {
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {
        // TODO: Improve here
        return empty();
    }

    let result_data = cache
        .topics
        .filter_clone(|&(ref c, _)| c == &cluster_id)
        .into_iter()
        .map(|((_, topic_name), partitions)| {
            let metrics = cache
                .metrics
                .get(&(cluster_id.clone(), topic_name.to_owned()))
                .unwrap_or_default()
                .aggregate_broker_metrics();
            TopicDetails {
                topic_name,
                partition_count: partitions.len(),
                errors: partitions
                    .into_iter()
                    .filter_map(|p| p.error)
                    .collect::<Vec<_>>()
                    .join(","),
                b_rate_15: metrics.b_rate_15.round(),
                m_rate_15: metrics.m_rate_15.round(),
            }
        })
        .collect::<Vec<_>>();

    json!({ "data": result_data }).to_string()
}

//
// ********** BROKERS LIST **********
//

#[get("/api/clusters/<cluster_id>/brokers")]
pub fn brokers(cluster_id: ClusterId, cache: &State<Cache>) -> String {
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {
        // TODO: Improve here
        return empty();
    }

    let brokers = brokers.unwrap();
    let broker_metrics = cache
        .metrics
        .get(&(cluster_id.to_owned(), "__TOTAL__".to_owned()))
        .unwrap_or_default();
    let mut result_data = Vec::with_capacity(brokers.len());
    for broker in brokers {
        let metric = broker_metrics
            .brokers
            .get(&broker.id)
            .cloned()
            .unwrap_or_default();
        result_data.push(json!((
            broker.id,
            broker.hostname,
            metric.b_rate_15.round(),
            metric.m_rate_15.round()
        )));
    }

    json!({ "data": result_data }).to_string()
}

//
// ********** GROUP **********
//

#[derive(Debug)]
struct GroupInfo {
    state: String,
    members: usize,
    topics: HashSet<TopicName>,
}

impl GroupInfo {
    fn new(state: String, members: usize) -> GroupInfo {
        GroupInfo {
            state,
            members,
            topics: HashSet::new(),
        }
    }

    fn new_empty() -> GroupInfo {
        GroupInfo {
            state: "Offsets only".to_owned(),
            members: 0,
            topics: HashSet::new(),
        }
    }

    fn add_topic(&mut self, topic_name: TopicName) {
        self.topics.insert(topic_name);
    }
}

// TODO: add doc
// TODO: add limit
fn build_group_list<F>(cache: &Cache, filter: F) -> HashMap<(ClusterId, String), GroupInfo>
where
    F: Fn(&ClusterId, &String) -> bool,
{
    let mut groups: HashMap<(ClusterId, String), GroupInfo> = cache.groups.lock_iter(|iter| {
        iter.filter(|&(&(ref c, ref g), _)| filter(c, g))
            .map(|(&(ref c, _), g)| {
                (
                    (c.clone(), g.name.clone()),
                    GroupInfo::new(g.state.clone(), g.members.len()),
                )
            })
            .collect()
    });

    let offsets = cache
        .offsets
        .filter_clone_k(|&(ref c, ref g, _)| filter(c, g));
    for (cluster_id, group, t) in offsets {
        groups
            .entry((cluster_id, group))
            .or_insert_with(GroupInfo::new_empty)
            .add_topic(t);
    }

    groups
}

#[get("/api/clusters/<cluster_id>/groups")]
pub fn cluster_groups(cluster_id: ClusterId, cache: &State<Cache>) -> String {
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {
        // TODO: Improve here
        return empty();
    }

    let groups = build_group_list(cache.inner(), |c, _| c == &cluster_id);

    let mut result_data = Vec::with_capacity(groups.len());
    for ((_cluster_id, group_name), info) in groups {
        result_data.push(json!((
            group_name,
            info.state,
            info.members,
            info.topics.len()
        )));
    }

    json!({ "data": result_data }).to_string()
}

#[get("/api/clusters/<cluster_id>/topics/<topic_name>/groups")]
pub fn topic_groups<'a>(cluster_id: ClusterId, topic_name: &'a str, cache: &State<Cache>) -> String {
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {
        // TODO: Improve here
        return empty();
    }

    let groups = build_group_list(cache.inner(), |c, _| c == &cluster_id);

    let mut result_data = Vec::with_capacity(groups.len());
    for ((_cluster_id, group_name), info) in groups {
        if !info.topics.contains(&topic_name.to_string()) {
            continue;
        }
        result_data.push(json!((
            group_name,
            info.state,
            info.members,
            info.topics.len()
        )));
    }

    json!({ "data": result_data }).to_string()
}

#[get("/api/clusters/<cluster_id>/groups/<group_name>/members")]
pub fn group_members<'a>(cluster_id: ClusterId, group_name: &'a str, cache: &State<Cache>) -> String {
    let group = cache
        .groups
        .get(&(cluster_id.clone(), group_name.to_string()));
    if group.is_none() {
        // TODO: Improve here
        return empty();
    }

    let group = group.unwrap();

    let mut result_data = Vec::with_capacity(group.members.len());
    for member in group.members {
        let assigns = member
            .assignments
            .iter()
            .map(|assign| {
                format!(
                    "{}/{}",
                    assign.topic,
                    assign
                        .partitions
                        .iter()
                        .map(i32::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        result_data.push(json!((
            member.id,
            member.client_id,
            member.client_host,
            assigns
        )));
    }

    json!({ "data": result_data }).to_string()
}

#[get("/api/clusters/<cluster_id>/groups/<group_name>/offsets")]
pub fn group_offsets<'a>(cluster_id: ClusterId, group_name: &'a str, cache: &State<Cache>) -> String {
    let offsets = cache.offsets_by_cluster_group(&cluster_id, group_name);

    let wms = time!("fetching wms", fetch_watermarks(&cluster_id, &offsets));
    let wms = match wms {
        Ok(wms) => wms,
        Err(e) => {
            error!("Error while fetching watermarks: {}", e);
            return empty();
        }
    };

    let mut result_data = Vec::with_capacity(offsets.len());
    for ((_cluster_id, _group, topic), partitions) in offsets {
        for (partition_id, &curr_offset) in partitions.iter().enumerate() {
            let (low, high) = match wms.get(&(topic.clone(), partition_id as i32)) {
                Some(&Ok((low_mark, high_mark))) => (low_mark, high_mark),
                _ => (-1, -1),
            };
            let (lag_shown, percentage_shown) = match (high - low, high - curr_offset) {
                (0, _) => ("Empty topic".to_owned(), "0.0%".to_owned()),
                (size, lag) if lag > size => ("Out of retention".to_owned(), "".to_owned()),
                (size, lag) => (
                    lag.to_string(),
                    format!("{:.1}%", (lag as f64) / (size as f64) * 100.0),
                ),
            };
            result_data.push(json!((
                topic.clone(),
                partition_id,
                high - low,
                low,
                high,
                curr_offset,
                lag_shown,
                percentage_shown
            )));
        }
    }

    json!({ "data": result_data }).to_string()
}

type ClusterGroupOffsets = ((ClusterId, String, TopicName), Vec<i64>);

fn fetch_watermarks(
    cluster_id: &ClusterId,
    offsets: &[ClusterGroupOffsets],
) -> Result<HashMap<TopicPartition, KafkaResult<(i64, i64)>>> {
    let consumer = CONSUMERS.get_err(cluster_id)?;

    let cpu_pool = Builder::new().pool_size(32).create();

    let mut futures = Vec::new();

    for &((_, _, ref topic), ref partitions) in offsets {
        for partition_id in 0..partitions.len() {
            let consumer_clone = consumer.clone();
            let topic_clone = topic.clone();
            let wm_future = cpu_pool.spawn_fn(move || {
                let wms = consumer_clone.fetch_watermarks(&topic_clone, partition_id as i32, rdkafka::util::Timeout::After(Duration::from_millis(10000)));
                Ok::<_, ()>(((topic_clone, partition_id as i32), wms)) // never fail
            });
            futures.push(wm_future);
        }
    }

    let watermarks = future::join_all(futures)
        .wait()
        .unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

    Ok(watermarks)
}

//
// ********** TOPIC TOPOLOGY **********
//

#[get("/api/clusters/<cluster_id>/topics/<topic_name>/topology")]
pub fn topic_topology<'a>(cluster_id: ClusterId, topic_name: &'a str, cache: &State<Cache>) -> String {
    let partitions = cache
        .topics
        .get(&(cluster_id.to_owned(), topic_name.to_string()));
    if partitions.is_none() {
        return empty();
    }

    let topic_metrics = cache
        .metrics
        .get(&(cluster_id.clone(), topic_name.to_string()))
        .unwrap_or_default();
    let partitions = partitions.unwrap();

    let mut result_data = Vec::with_capacity(partitions.len());
    for p in partitions {
        let partition_metrics = topic_metrics
            .brokers
            .get(&p.leader)
            .and_then(|broker_metrics| broker_metrics.partitions.get(p.id as usize))
            .cloned()
            .unwrap_or_default();
        result_data.push(json!((
            p.id,
            partition_metrics.size_bytes,
            p.leader,
            p.replicas,
            p.isr,
            p.error
        )));
    }

    json!({ "data": result_data }).to_string()
}

//
// ********** SEARCH **********
//

#[get("/api/search/consumer?<search..>")]
pub fn consumer_search(search: OmnisearchFormParams, cache: &State<Cache>) -> String {
    let groups = if search.regex {
        Regex::new(&search.string)
            .map(|r| build_group_list(&cache, |_, g| r.is_match(g)))
            .unwrap_or_default()
    } else {
        build_group_list(&cache, |_, g| g.contains(&search.string))
    };

    let mut result_data = Vec::with_capacity(groups.len());
    for ((cluster_id, group_name), info) in groups {
        result_data.push(json!((
            cluster_id,
            group_name,
            info.state,
            info.members,
            info.topics.len()
        )));
    }

    json!({ "data": result_data }).to_string()
}

#[get("/api/search/topic?<search..>")]
pub fn topic_search(search: OmnisearchFormParams, cache: &State<Cache>) -> String {
    let topics = if search.regex {
        Regex::new(&search.string)
            .map(|r| cache.topics.filter_clone(|&(_, ref name)| r.is_match(name)))
            .unwrap_or_default()
    } else {
        cache
            .topics
            .filter_clone(|&(_, ref name)| name.contains(&search.string))
    };

    let mut result_data = Vec::new();
    for ((cluster_id, topic_name), partitions) in topics {
        let metrics = cache
            .metrics
            .get(&(cluster_id.clone(), topic_name.clone()))
            .unwrap_or_default()
            .aggregate_broker_metrics();
        let errors = partitions.iter().find(|p| p.error.is_some());
        result_data.push(json!((
            cluster_id,
            topic_name,
            partitions.len(),
            errors,
            metrics.b_rate_15,
            metrics.m_rate_15
        )));
    }

    json!({ "data": result_data }).to_string()
}

//
// ********** ZOOKEEPER **********
//

#[get("/api/clusters/<cluster_id>/reassignment")]
pub async fn cluster_reassignment(
    cluster_id: ClusterId,
    cache: &State<Cache>,
    config: &State<Config>,
) -> String {
    if cache.brokers.get(&cluster_id).is_none() {
        return empty();
    }

    let zk_url = &config.clusters.get(&cluster_id).unwrap().zookeeper;
    info!("zk url={}",&zk_url);
    let zk = match ZK::new(zk_url).await {
        // TODO: cache ZK clients
        
        Ok(zk) => zk,
        Err(_) => {
            error!("Error connecting to {:?}", zk_url);
            return empty();
        }
    };

    let reassignment = match zk.pending_reassignment().await {
        Some(reassignment) => reassignment,
        None => return empty(),
    };

    let result_data = reassignment
        .partitions
        .into_iter()
        .map(|p| {
            let topic_metrics = cache
                .metrics
                .get(&(cluster_id.clone(), p.topic.to_owned()))
                .unwrap_or_default();

            let replica_metrics = p
                .replicas
                .iter()
                .map(|ref r| {
                    topic_metrics
                        .brokers
                        .get(&r)
                        .and_then(|b| b.partitions.get(p.partition as usize))
                        .cloned()
                        .unwrap_or_default()
                        .size_bytes
                })
                .collect::<Vec<_>>();

            json!((p.topic, p.partition, p.replicas, replica_metrics))
        })
        .collect::<Vec<_>>();

    json!({ "data": result_data }).to_string()
}

//
// ********** INTERNALS **********
//

#[get("/api/internals/cache/brokers")]
pub fn cache_brokers(cache: &State<Cache>) -> String {
    let result_data = cache.brokers.lock_iter(|brokers_cache_entry| {
        brokers_cache_entry
            .map(|(cluster_id, brokers)| {
                (
                    cluster_id.clone(),
                    brokers.iter().map(|b| b.id).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    });

    json!({ "data": result_data }).to_string()
}

#[get("/api/internals/cache/metrics")]
pub fn cache_metrics(cache: &State<Cache>) -> String {
    let result_data = cache.metrics.lock_iter(|metrics_cache_entry| {
        metrics_cache_entry
            .map(|(&(ref cluster_id, ref topic_id), metrics)| {
                (cluster_id.clone(), topic_id.clone(), metrics.brokers.len())
            })
            .collect::<Vec<_>>()
    });

    json!({ "data": result_data }).to_string()
}

#[get("/api/internals/cache/offsets")]
pub fn cache_offsets(cache: &State<Cache>) -> String {
    let result_data = cache.offsets.lock_iter(|offsets_cache_entry| {
        offsets_cache_entry
            .map(
                |(&(ref cluster_id, ref group_name, ref topic_id), partitions)| {
                    (
                        cluster_id.clone(),
                        group_name.clone(),
                        topic_id.clone(),
                        format!("{:?}", partitions),
                    )
                },
            )
            .collect::<Vec<_>>()
    });

    json!({ "data": result_data }).to_string()
}

#[get("/api/internals/live_consumers")]
pub fn live_consumers(live_consumers: &State<LiveConsumerStore>) -> String {
    let result_data = live_consumers
        .consumers()
        .iter()
        .map(|consumer| {
            (
                consumer.id(),
                consumer.cluster_id().to_owned(),
                consumer.topic().to_owned(),
                consumer.last_poll().elapsed().as_secs(),
            )
        })
        .collect::<Vec<_>>();
    json!({ "data": result_data }).to_string()
}

fn empty() -> String {
    json!({"data": []}).to_string()
}
