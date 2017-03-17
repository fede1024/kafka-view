use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};
use futures_cpupool::Builder;
use futures::{future, Future};
use rdkafka::error::KafkaResult;

use cache::Cache;
use web_server::server::CacheType;
use metrics::build_topic_metrics;
use utils::json_gzip_response;
use offsets::OffsetStore;
use metadata::{CONSUMERS, ClusterId, TopicName};
use error::*;

use std::collections::HashMap;

//
// ********** TOPICS LIST **********
//

pub fn cluster_topics(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();

    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
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

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}

//
// ********** BROKERS LIST **********
//

pub fn cluster_brokers(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();

    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let brokers = brokers.unwrap();
    let mut result_data = Vec::with_capacity(brokers.len());
    for broker in brokers {
        let rate = cache.metrics.get(&(cluster_id.to_owned(), broker.id))
            .and_then(|b_metrics| { b_metrics.topics.get("__TOTAL__").cloned() })
            .unwrap_or((-1f64, -1f64)); // TODO null instead?
        result_data.push(json!((broker.id, broker.hostname, rate.0.round(), rate.1.round())));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
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

fn build_group_list(cache: &Cache, cluster_id: &ClusterId, topic: Option<&str>) -> HashMap<String, GroupInfo> {
    let mut groups = HashMap::new();
    let registered_groups_map = match topic {
        Some(topic) => cache.groups.filter_clone_v(|&(ref c, ref t)| c == cluster_id && t == topic),
        None => cache.groups.filter_clone_v(|&(ref c, _)| c == cluster_id),
    };

    for group in registered_groups_map {
        let group_result = GroupInfo::new(group.state, group.members.len());
        groups.insert(group.name, group_result);
    }

    let offsets = match topic {
        Some(topic) => cache.offsets_by_cluster_topic(&cluster_id.to_owned(), &topic.to_owned()),
        None => cache.offsets_by_cluster(&cluster_id.to_owned()),
    };

    for ((_, group, _), _) in offsets {
        (*groups.entry(group).or_insert(GroupInfo::new_empty())).add_offset();
    }
    return groups;
}

pub fn cluster_groups(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();

    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let groups = build_group_list(cache, &cluster_id, None);

    let mut result_data = Vec::with_capacity(groups.len());
    for (group_name, info) in groups {
        result_data.push(json!((group_name, info.state, info.members, info.stored_offsets)));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}

pub fn topic_groups(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();
    let topic_name = req.extensions.get::<Router>().unwrap().find("topic_name").unwrap();

    let brokers = cache.brokers.get(&cluster_id);
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let groups = build_group_list(cache, &cluster_id, Some(topic_name));

    let mut result_data = Vec::with_capacity(groups.len());
    for (group_name, info) in groups {
        result_data.push(json!((group_name, info.state, info.members, info.stored_offsets)));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}

pub fn group_members(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id: ClusterId = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();
    let group_name = req.extensions.get::<Router>().unwrap().find("group_name").unwrap();

    let group = cache.groups.get(&(cluster_id.clone(), group_name.to_owned()));
    if group.is_none() {  // TODO: Improve here
        return Ok(json_gzip_response(json!({"data": []})));
    }

    let group = group.unwrap();

    let mut result_data = Vec::with_capacity(group.members.len());
    for member in group.members {
        result_data.push(json!((member.id, member.client_id, member.client_host)));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}

pub fn group_offsets(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();
    let group_name = req.extensions.get::<Router>().unwrap().find("group_name").unwrap();

    let offsets = cache.offsets_by_cluster_group(&cluster_id, &group_name.to_owned());


    let wms = time!("fetch wms", fetch_watermarks(&cluster_id, &offsets));
    let wms = match wms {
        Ok(wms) => wms,
        Err(e) => {
            error!("Error while fetching watermarks: {}", e);
            return Ok(json_gzip_response(json!({})));  // TODO: show error to user?
        }
    };

    let mut result_data = Vec::with_capacity(offsets.len());
    for ((_, group, topic), partitions) in offsets {
        for (partition_id, &offset) in partitions.iter().enumerate() {
            let (low, high, lag) = match wms.get(&(topic.clone(), partition_id as i32)) {
                Some(&Ok((low_mark, high_mark))) => (low_mark, high_mark, high_mark - offset),
                _ => (-1, -1, -1),
            };
            let lag_shown = match (high, offset - low) {
                (0, _) => "Empty topic".to_owned(),
                (_, lag) if lag < 0 => "Out of retention".to_owned(),
                _ => lag.to_string()
            };
            result_data.push(json!((topic.clone(), partition_id, low, high, offset, lag_shown)));
        }
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}

fn fetch_watermarks(cluster_id: &ClusterId, offsets: &Vec<((ClusterId, String, TopicName), Vec<i64>)>)
        -> Result<HashMap<(TopicName, i32), KafkaResult<(i64, i64)>>> {
    let consumer = match CONSUMERS.read() {
        Ok(ref cache) => match cache.get(&cluster_id) {
            Some(consumer_arc) => consumer_arc.clone(),
            None => bail!("No consumer found for {}", cluster_id),
        },
        Err(_) => panic!("Poison err"),
    };

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

pub fn topic_topology(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id: ClusterId = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap().into();
    let topic_name = req.extensions.get::<Router>().unwrap().find("topic_name").unwrap();

    let partitions = cache.topics.get(&(cluster_id.to_owned(), topic_name.to_owned()));
    if partitions.is_none() {
        return Ok(Response::with((status::NotFound, "")));
    }

    let partitions = partitions.unwrap();

    let mut result_data = Vec::with_capacity(partitions.len());
    for p in partitions {
        result_data.push(json!((p.id, p.leader, p.replicas, p.isr, p.error)));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}
