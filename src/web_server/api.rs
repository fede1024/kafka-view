use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};

use cache::{MetricsCache, Cache};
use web_server::server::{CacheType, ConfigArc, RequestTimer};
use metrics::build_topic_metrics;
use utils::json_gzip_response;
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
        let def = (-1f64, -1f64);
        let rate = topic_metrics.get(topic_name).unwrap_or(&def);
        let errors = partitions.iter().find(|p| p.error.is_some());
        // let err_str = format!("{:?}", errors);
        result_data.push(json!((topic_name, partitions.len(), &errors, rate.0.round(), rate.1.round())));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
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

    // let groups = cache.groups.filter_clone(|&(ref c, _), _| c == cluster_id);

    let mut result_data = Vec::with_capacity(consumer_offsets.len());
    for (group_name, &topic_count) in consumer_offsets.iter() {
        result_data.push(json!((group_name, topic_count, "TODO")));
    }

    let result = json!({"data": result_data});
    Ok(json_gzip_response(result))
}
