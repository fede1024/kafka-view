use futures::{future, Future};
use futures_cpupool::Builder;
use rdkafka::error::KafkaResult;
use regex::Regex;
use rocket::State;

use cache::Cache;
use error::*;
use metadata::{CONSUMERS, ClusterId, TopicName};
use metrics::build_topic_metrics;
use offsets::OffsetStore;
use web_server::pages::omnisearch::OmnisearchFormParams;

use std::collections::HashMap;

//
// ********** TOPICS LIST **********
//

#[get("/api/clusters/<cluster_id>/topics?<timestamp>")]
pub fn cluster_topics(cluster_id: ClusterId, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return json!({"data": []}).to_string();
    }

    let brokers = brokers.unwrap();
    let topics = cache.topics.filter_clone(|&(ref c, _)| c == &cluster_id);
    let topic_metrics = build_topic_metrics(&cluster_id, &brokers, topics.len(), &cache.metrics);

    let mut result_data = Vec::with_capacity(topics.len());
    for &((_, ref topic_name), ref partitions) in topics.iter() {
        let def = (-1f64, -1f64);
        let rate = topic_metrics.get(topic_name).unwrap_or(&def);
        let errors = partitions.iter().find(|p| p.error.is_some());
        // let err_str = format!("{:?}", errors);
        result_data.push(json!((topic_name, partitions.len(), &errors, rate.0.round(), rate.1.round())));
    }

    //Ok(json_gzip_response(json!({"data": result_data})))
    json!({"data": result_data}).to_string()
}

//
// ********** BROKERS LIST **********
//

#[get("/api/clusters/<cluster_id>/brokers?<timestamp>")]
pub fn brokers(cluster_id: ClusterId, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return json!({"data": []}).to_string();
    }

    let brokers = brokers.unwrap();
    let mut result_data = Vec::with_capacity(brokers.len());
    for broker in brokers {
        let rate = cache.metrics.get(&(cluster_id.to_owned(), broker.id))
            .and_then(|b_metrics| { b_metrics.topics.get("__TOTAL__").cloned() })
            .unwrap_or((-1f64, -1f64)); // TODO null instead?
        result_data.push(json!((broker.id, broker.hostname, rate.0.round(), rate.1.round())));
    }

    json!({"data": result_data}).to_string()
}

//
// ********** GROUP **********
//

struct GroupInfo {
    state: String,
    members: usize,
    stored_offsets: usize,
}

impl GroupInfo {
    fn new(state: String, members: usize) -> GroupInfo {
        GroupInfo { state: state, members: members, stored_offsets: 0 }
    }

    fn new_empty() -> GroupInfo {
        GroupInfo { state: "Offsets only".to_owned(), members: 0, stored_offsets: 0 }
    }

    fn add_offset(&mut self) {
        self.stored_offsets += 1;
    }
}

// TODO: add doc
// TODO: add limit
fn build_group_list<F>(cache: &Cache, filter_fn: F) -> HashMap<(ClusterId, String), GroupInfo>
        where F: Fn(&ClusterId, &TopicName, &String) -> bool {

    let mut groups: HashMap<(ClusterId, String), GroupInfo> = cache.groups
        .lock_iter(|iter| {
            iter.filter(|&(&(ref c, ref t), ref g)| filter_fn(&c, &t, &g.name))
                .map(|(&(ref c, _), g)| ((c.clone(), g.name.clone()), GroupInfo::new(g.state.clone(), g.members.len())))
                .collect()
        });

    let offsets = cache.offsets.filter_clone_k(|&(ref c, ref g, ref t)| filter_fn(c, t, g));
    for (cluster_id, group, _) in offsets {
        (*groups.entry((cluster_id, group)).or_insert(GroupInfo::new_empty())).add_offset();
    }

    groups
}

#[get("/api/clusters/<cluster_id>/groups?<timestamp>")]
pub fn cluster_groups(cluster_id: ClusterId, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return json!({"data": []}).to_string();
    }

    let groups = build_group_list(cache.inner(), |c, _, _| &cluster_id == c);

    let mut result_data = Vec::with_capacity(groups.len());
    for ((_cluster_id, group_name), info) in groups {
        result_data.push(json!((group_name, info.state, info.members, info.stored_offsets)));
    }

    json!({"data": result_data}).to_string()
}

#[get("/api/clusters/<cluster_id>/topics/<topic_name>/groups?<timestamp>")]
pub fn topic_groups(cluster_id: ClusterId, topic_name: &str, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return json!({"data": []}).to_string();
    }

    let groups = build_group_list(cache.inner(), |c, t, _| &cluster_id == c && topic_name == t);

    let mut result_data = Vec::with_capacity(groups.len());
    for ((_cluster_id, group_name), info) in groups {
        result_data.push(json!((group_name, info.state, info.members, info.stored_offsets)));
    }

    json!({"data": result_data}).to_string()
}

#[get("/api/clusters/<cluster_id>/groups/<group_name>/members?<timestamp>")]
pub fn group_members(cluster_id: ClusterId, group_name: &str, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let group = cache.groups.get(&(cluster_id.clone(), group_name.to_owned()));
    if group.is_none() {  // TODO: Improve here
        return json!({"data": []}).to_string();
    }

    let group = group.unwrap();

    let mut result_data = Vec::with_capacity(group.members.len());
    for member in group.members {
        result_data.push(json!((member.id, member.client_id, member.client_host)));
    }

    json!({"data": result_data}).to_string()
}

#[get("/api/clusters/<cluster_id>/groups/<group_name>/offsets?<timestamp>")]
pub fn group_offsets(cluster_id: ClusterId, group_name: &str, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let offsets = cache.offsets_by_cluster_group(&cluster_id, &group_name.to_owned());

    let wms = time!("fetch wms", fetch_watermarks(&cluster_id, &offsets));
    let wms = match wms {
        Ok(wms) => wms,
        Err(e) => {
            error!("Error while fetching watermarks: {}", e);
            return json!({"data": []}).to_string();
        }
    };

    let mut result_data = Vec::with_capacity(offsets.len());
    for ((_cluster_id, _group, topic), partitions) in offsets {
        for (partition_id, &curr_offset) in partitions.iter().enumerate() {
            let (low, high) = match wms.get(&(topic.clone(), partition_id as i32)) {
                Some(&Ok((low_mark, high_mark))) => (low_mark, high_mark),
                _ => (-1, -1),
            };
            let (lag_shown, perc_shown) = match (high - low, high - curr_offset) {
                (_, lag) if lag < 0 => ("Out of retention".to_owned(), "".to_owned()),
                (0, lag) => (lag.to_string(), "0.0%".to_owned()),
                (size, lag) => (lag.to_string(), format!("{:.1}%", (lag as f64) / (size as f64) * 100.0))
            };
            result_data.push(json!((topic.clone(), partition_id, high-low, low, high, curr_offset, lag_shown, perc_shown)));
        }
    }

    json!({"data": result_data}).to_string()
}

fn fetch_watermarks(cluster_id: &ClusterId, offsets: &Vec<((ClusterId, String, TopicName), Vec<i64>)>)
        -> Result<HashMap<(TopicName, i32), KafkaResult<(i64, i64)>>> {
    let consumer = CONSUMERS.get_err(&cluster_id)?;

    let cpu_pool = Builder::new().pool_size(32).create();

    let mut futures = Vec::new();

    for &((_, _, ref topic), ref partitions) in offsets {
        for partition_id in 0..partitions.len() {
            let consumer_clone = consumer.clone();
            let topic_clone = topic.clone();
            let wm_future = cpu_pool.spawn_fn(move || {
                let wms = consumer_clone.fetch_watermarks(&topic_clone, partition_id as i32, 10000);
                Ok::<_, ()>(((topic_clone, partition_id as i32), wms))  // never fail
            });
            futures.push(wm_future);
        }
    }

    let watermarks = future::join_all(futures).wait().unwrap()
        .into_iter()
        .collect::<HashMap<_, _>>();

    Ok(watermarks)
}

//
// ********** TOPIC TOPOLOGY **********
//

#[get("/api/clusters/<cluster_id>/topics/<topic_name>/topology?<timestamp>")]
pub fn topic_topology(cluster_id: ClusterId, topic_name: &str, cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let partitions = cache.topics.get(&(cluster_id.to_owned(), topic_name.to_owned()));
    if partitions.is_none() {
        return json!({"data": []}).to_string();
    }

    let partitions = partitions.unwrap();

    let mut result_data = Vec::with_capacity(partitions.len());
    for p in partitions {
        result_data.push(json!((p.id, p.leader, p.replicas, p.isr, p.error)));
    }

    json!({"data": result_data}).to_string()
}

//
// ********** SEARCH **********
//

#[get("/api/search/consumer?<search>")]
pub fn consumer_search(search: OmnisearchFormParams, cache: State<Cache>) -> String {
    let groups = if search.regex {
        Regex::new(&search.string)
            .map(|r| build_group_list(&cache, |_, _, g| r.is_match(g)))
            .unwrap_or(HashMap::new())
    } else {
        build_group_list(&cache, |_, _, g| g.contains(&search.string))
    };

    let mut result_data = Vec::with_capacity(groups.len());
    for ((cluster_id, group_name), info) in groups {
        result_data.push(json!((cluster_id, group_name, info.state, info.members, info.stored_offsets)));
    }

    json!({"data": result_data}).to_string()
}

#[get("/api/search/topic?<search>")]
pub fn topic_search(search: OmnisearchFormParams, cache: State<Cache>) -> String {
    let topics = if search.regex {
        Regex::new(&search.string)
            .map(|r| cache.topics.filter_clone(|&(_, ref name)| r.is_match(name)))
            .unwrap_or(Vec::new())
    } else {
        cache.topics.filter_clone(|&(_, ref name)| name.contains(&search.string))
    };

    let mut metrics_map = HashMap::new();
    let mut result_data = Vec::new();
    for ((cluster_id, topic_name), partitions) in topics {
        let cluster_metrics = metrics_map.entry(cluster_id.clone())
            .or_insert_with(|| {
                cache.brokers.get(&cluster_id)
                    .map(|brokers| build_topic_metrics(&cluster_id, &brokers, 100, &cache.metrics))
            });
        let (b_rate, m_rate) = cluster_metrics.as_ref()
            .and_then(|c_metrics| c_metrics.get(&topic_name).cloned())
            .unwrap_or((-1f64, -1f64));
        let errors = partitions.iter().find(|p| p.error.is_some());
        result_data.push(json!((cluster_id, topic_name, partitions.len(), errors, b_rate, m_rate)));
    }

    json!({"data": result_data}).to_string()
}

//
// ********** INTERNALS **********
//

#[get("/api/internals/cache/brokers?<timestamp>")]
pub fn cache_brokers(cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let result_data = cache.brokers.lock_iter(|brokers_cache_entry| {
        brokers_cache_entry.map(|(cluster_id, brokers)| {
            ((cluster_id.clone(), brokers.iter().map(|b| b.id).collect::<Vec<_>>()))
        })
        .collect::<Vec<_>>()
    });

    json!({"data": result_data}).to_string()
}

#[get("/api/internals/cache/metrics?<timestamp>")]
pub fn cache_metrics(cache: State<Cache>, timestamp: &str) -> String {
    let _ = timestamp;
    let result_data = cache.metrics.lock_iter(|metrics_cache_entry| {
        metrics_cache_entry
            .map(|(&(ref cluster_id, ref broker_id), metrics)| {
                ((cluster_id.clone(), broker_id.clone(), metrics.topics.len()))
            }).collect::<Vec<_>>()
    });

    json!({"data": result_data}).to_string()
}
