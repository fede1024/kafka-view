use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};

use cache::{MetricsCache, Cache};
use web_server::server::{CacheType, ConfigArc, RequestTimer};
use metrics::build_topic_metrics;
use utils::json_response;
use offsets::OffsetStore;

use std::collections::HashMap;

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
        let rate = topic_metrics.get(topic_name)
            .map(|r| (format!("{:.1} KB/s", (r.0 / 1000f64)), format!("{:.0} msg/s", r.1)))
            .unwrap_or(("no data".to_string(), "".to_string()));
        let errors = partitions.iter().map(|p| (p.id, p.error.clone())).filter(|&(_, ref error)| error.is_some()).collect::<Vec<_>>();
        let err_str = if errors.len() == 0 { // TODO use different format
            "OK"
        } else {
            "ERR"
        };
        result_data.push(json!((topic_name, partitions.len(), err_str, rate.0, rate.1)));
    }

    let result = json!({"data": result_data});
    Ok(json_response(result))
}

pub fn cluster_brokers(req: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "")))
}

pub fn cluster_groups(req: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "")))
}

pub fn cluster_offsets(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let brokers = cache.brokers.get(&cluster_id.to_owned());
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let mut consumer_offsets = HashMap::new();
    for ((_, group, _), _) in cache.offsets_by_cluster(&cluster_id.to_owned()) {
        *consumer_offsets.entry(group).or_insert(0) += 1;
    }

    let mut result_data = Vec::with_capacity(consumer_offsets.len());
    for (group_name, &topic_count) in consumer_offsets.iter() {
        result_data.push(json!((group_name, topic_count, "TODO")));
    }

    let result = json!({"data": result_data});
    Ok(json_response(result))
}
