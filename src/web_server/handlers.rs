use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use staticfile::Static;
use mount;
use maud::PreEscaped;
use router::Router;

use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

use web_server::server::CacheType;
use web_server::view::layout;
use metadata::Metadata;


use cache::MetricsCache;

use chrono::UTC;


fn format_broker_list(brokers: &Vec<i32>) -> String {
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(cluster_id: &str, metadata: Arc<Metadata>, topic_metrics: HashMap<String, f64>) -> PreEscaped<String> {
    let title = html! { "Metadata for cluster: " (cluster_id) };
    let content = html! {
        p { "Last update: " (metadata.refresh_time) }
        ol {
            @for (topic_name, partitions) in &metadata.topics {
                @let rate = topic_metrics.get(topic_name).unwrap_or(&-1f64) / 1000f64 {
                    li  { (topic_name) " - 15 min rate: " (format!("{:.1} KB/s", rate)) }
                    ul {
                        @for partition in partitions {
                            li { (partition.id) " - " (partition.leader) " " (format_broker_list(&partition.isr)) }
                        }
                    }
                }
            }
        }
    };
    layout::panel(title, content)
}

fn build_topic_metrics(cluster_id: &str, metadata: &Metadata, metrics: &MetricsCache) -> HashMap<String, f64> {
    let mut result: HashMap<String, f64> = HashMap::with_capacity(metadata.topics.len());
    for broker in &metadata.brokers {
        if let Some(broker_metrics) = metrics.get(&(cluster_id.to_owned(), broker.id)) {
            for (topic_name, rate) in broker_metrics.topics {
                if rate < 0.001 {
                    continue;
                }
                // Keep an eye on RFC 1769
                *result.entry(topic_name.to_owned()).or_insert(0f64) += rate;
            }
        }
    }
    result
}

pub fn home_handler(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();

    let mut content = "".to_string();
    for cluster_id in cache.metadata.keys() {
        let metadata = cache.metadata.get(&cluster_id.to_string()).unwrap();
        let topic_metrics = build_topic_metrics(&cluster_id, &metadata, &cache.metrics);
        content += &format_metadata(&cluster_id, metadata, topic_metrics).into_string();
    }

    let html = layout::page("Clusters", PreEscaped(content));

    Ok(Response::with((status::Ok, html)))
}

pub fn cluster_handler(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let metadata = cache.metadata.get(&cluster_id.to_owned());
    if metadata.is_none() {
        let content = layout::notification("danger", html! { "The specified cluster doesn't exist." });
        let page_title = format!("Cluster: {}", cluster_id);
        let html = layout::page(&page_title, content);
        return Ok(Response::with((status::Ok, html)));
    }

    // let mut content = "".to_string();
    // for cluster_id in cache.metadata.keys() {
    //     let metadata = cache.metadata.get(&cluster_id.to_string()).unwrap();
    //     let topic_metrics = build_topic_metrics(&cluster_id, &metadata, &cache.metrics);
    //     content += &format_metadata(&cluster_id, metadata, topic_metrics).into_string();
    // }

    let heads = vec![html!("Name"), html!("Partitions"), html!("Traffic"), html!("Errors")];
    let rows = vec![
        vec![html!("Name"), html!("Partitions"), html!("Traffic"), html!("Errors")],
        vec![html!("Name"), html!("Partitions"), html!("Traffic"), html!("Errors")],
        vec![html!("Name"), html!("Partitions"), html!("Traffic"), html!("Errors")],
        vec![html!("Name"), html!("Partitions"), html!("Traffic"), html!("Errors")],
    ];

    let content = layout::panel(html!((cluster_id)), layout::table(heads.iter(), rows.iter())).into_string();

    let page_title = format!("Cluster: {}", cluster_id);
    let html = layout::page(&page_title, PreEscaped(content));

    Ok(Response::with((status::Ok, html)))
}

pub struct AssetsHandler;

impl AssetsHandler {
    pub fn new(prefix: &str, mount_path: &str) -> mount::Mount {
        let mut assets_mount = mount::Mount::new();
        assets_mount.mount(prefix, Static::new(Path::new(mount_path)));
        assets_mount
    }
}
