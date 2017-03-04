use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};
use maud::PreEscaped;

use web_server::server::CacheType;
use web_server::view::layout;
use web_server::pages;
use cache::MetricsCache;
use metrics::build_topic_metrics;

use std::collections::HashMap;


fn topic_table(cluster_id: &str, topic_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topic/{}/topology", cluster_id, topic_name);
    layout::datatable_ajax("topology-ajax", &api_url, cluster_id,
        html! { tr { th "Id" th "Leader" th "Replicas" th "ISR" th "Status" } }
    )
}

fn consumer_groups_table(cluster_id: &str, topic_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topic/{}/groups", cluster_id, topic_name);
    layout::datatable_ajax("groups-ajax", &api_url, cluster_id,
           html! { tr { th "Group name" th "#Members" th "Status" th "Stored topic offsets" } },
    )
}

// TODO: simplify?
pub fn topic_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();
    let topic_name = req.extensions.get::<Router>().unwrap().find("topic_name").unwrap();

    let partitions = match cache.topics.get(&(cluster_id.to_owned(), topic_name.to_owned())) {
        Some(partitions) => partitions,
        None => {
            return pages::warning_page(req,
                &format!("Topic: {}", cluster_id),
                "The specified cluster doesn't exist.")
        }
    };

    let brokers = cache.brokers.get(&cluster_id.to_owned()).expect("Broker should exist");

    // TODO: create function specific for single topic metrics
    let metrics = build_topic_metrics(&cluster_id, &brokers, 100, &cache.metrics)
        .get(topic_name).cloned();
    let content = html! {
        h3 style="margin-top: 0px" "General information"
        dl class="dl-horizontal" {
            dt "Cluster name " dd (cluster_id)
            dt "Topic name " dd (topic_name)
            dt "Number of partitions " dd (partitions.len())
            dt "Number of replicas " dd (partitions[0].replicas.len())
            @if metrics.is_some() {
                dt "Traffic last 15 minutes"
                dd (format!("{:.1} KB/s {:.0} msg/s", metrics.unwrap().0 / 1000f64, metrics.unwrap().1))
            } @else {
                dt "Traffic data" dd "Not available"
            }
        }
        h3 "Topology"
        (topic_table(cluster_id, topic_name))
        h3 "Consumer groups"
        (consumer_groups_table(cluster_id, topic_name))
    };

    let html = layout::page(req, &format!("Topic: {}", topic_name), content);

    Ok(Response::with((status::Ok, html)))
}

