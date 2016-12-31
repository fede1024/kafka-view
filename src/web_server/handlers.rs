use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use staticfile::Static;
use mount;
use maud::PreEscaped;

use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

use web_server::server::CacheType;
use web_server::view::layout;
use metadata::Metadata;

use cache::MetricsCache;


fn format_broker_list(brokers: &Vec<i32>) -> String {
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(cluster_id: &str, metadata: Arc<Metadata>, topic_metrics: HashMap<String, f64>) -> PreEscaped<String> {
    html! {
        h2 { "Metadata for " (cluster_id) }
        p { "Last update: " (metadata.refresh_time) }
        ol {
            @for (topic_name, partitions) in &metadata.topics {
                @let rate = topic_metrics.get(topic_name).unwrap_or(&-1f64) / 1000f64 {
                    @if rate > 0.1f64 {
                        li  { (topic_name) " - 15 min rate: " (format!("{:.1} KB/s", rate)) }
                        ul {
                            @for partition in partitions {
                                li { (partition.id) " - " (partition.leader) " " (format_broker_list(&partition.isr)) }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn build_topic_metrics(cluster_id: &str, metadata: &Metadata, metrics: MetricsCache) -> HashMap<String, f64> {
    let mut result = HashMap::new();
    for broker in &metadata.brokers {
        if let Some(broker_metrics) = metrics.get(&(cluster_id.to_owned(), broker.id)) {
            for (topic_name, _) in &metadata.topics {
                if let Some(rate) = broker_metrics.topics.get(topic_name) {
                    *result.entry(topic_name.to_owned()).or_insert(0f64) += *rate;
                }
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
        let topic_metrics = build_topic_metrics(&cluster_id, &metadata, cache.metrics.alias());
        content += &format_metadata(&cluster_id, metadata, topic_metrics).into_string();
    }

    let html = layout::page("Clusters", PreEscaped(content));

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
