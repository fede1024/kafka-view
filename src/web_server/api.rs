use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};

use cache::{MetricsCache, Cache};
use web_server::server::{CacheType, ConfigArc, RequestTimer};
use metrics::build_topic_metrics;
use utils::json_gzip_response;
use offsets::OffsetStore;

use std::collections::HashMap;

//
// ********** TOPICS LIST **********
//

pub fn cluster_topics(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let brokers = cache.brokers.get(&cluster_id.to_owned());
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let brokers = brokers.unwrap();
    let topics = cache.topics.filter_clone(|&(ref c, _), _| c == cluster_id);
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
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let brokers = cache.brokers.get(&cluster_id.to_owned());
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
// ********** GROUP LIST **********
//

struct GroupInfo(usize, String, usize);

impl GroupInfo {
    fn new(members: usize, state: String) -> GroupInfo {
        GroupInfo(members, state, 0)
    }

    fn new_empty() -> GroupInfo {
        GroupInfo(0, "No group".to_owned(), 0)
    }

    fn add_offset(&mut self) {
        self.2 += 1;
    }
}

pub fn cluster_groups(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let brokers = cache.brokers.get(&cluster_id.to_owned());
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let mut groups = HashMap::new();
    for (_, group) in cache.groups.filter_clone(|&(ref c, _), _| c == cluster_id) {
        let group_result = GroupInfo::new(group.members.len(), group.state);
        groups.insert(group.name, group_result);
    }

    for ((_, group, _), _) in cache.offsets_by_cluster(&cluster_id.to_owned()) {
        (*groups.entry(group).or_insert(GroupInfo::new_empty())).add_offset();
    }

    let mut result_data = Vec::with_capacity(groups.len());
    for (group_name, group_info) in groups {
        result_data.push(json!((group_name, group_info.0, group_info.1, group_info.2)));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}
