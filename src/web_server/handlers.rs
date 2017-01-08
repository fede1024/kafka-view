use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use staticfile::Static;
use mount;
use maud::PreEscaped;
use router::Router;
use chrono::UTC;

use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

use web_server::server::CacheType;
use web_server::view::layout;
use metadata::{Metadata, Broker, Partition};
use cache::MetricsCache;


fn format_broker_list(brokers: &Vec<i32>) -> String {
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(cluster_id: &str, metadata: Arc<Metadata>, topic_metrics: HashMap<String, (f64, f64)>) -> PreEscaped<String> {
    let title = html! { "Metadata for cluster: " (cluster_id) };
    let content = html! {
        p { "Last update: " (metadata.refresh_time) }
        ol {
            @for (topic_name, partitions) in &metadata.topics {
                @let rate = topic_metrics.get(topic_name).unwrap_or(&(-1f64, -1f64)).clone() {
                    li  { (topic_name) " - 15 min rate: " (format!("{:.1} KB/s", rate.0 / 1000f64)) }
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

fn build_topic_metrics(cluster_id: &str, metadata: &Metadata, metrics: &MetricsCache) -> HashMap<String, (f64, f64)> {
    let mut result = HashMap::with_capacity(metadata.topics.len());
    for broker in &metadata.brokers {
        if let Some(broker_metrics) = metrics.get(&(cluster_id.to_owned(), broker.id)) {
            for (topic_name, rate) in broker_metrics.topics {
                // Keep an eye on RFC 1769
                let mut entry_ref = result.entry(topic_name.to_owned()).or_insert((0f64, 0f64));
                *entry_ref = (entry_ref.0 + rate.0, entry_ref.1 + rate.1);
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

    let clusters = cache.metadata.keys();
    let html = layout::page("Clusters", &clusters, PreEscaped(content));

    Ok(Response::with((status::Ok, html)))
}

fn broker_table_row(cluster_id: &str, broker: &Broker, metrics: &MetricsCache) -> PreEscaped<String> {
    let byte_rate = metrics.get(&(cluster_id.to_owned(), broker.id))
        .and_then(|broker_metrics| { broker_metrics.topics.get("__TOTAL__").cloned() })
        .map(|r| format!("{:.1} KB/s", (r.0 / 1000f64)))
        .unwrap_or("no data".to_string());
    let msg_rate = metrics.get(&(cluster_id.to_owned(), broker.id))
        .and_then(|broker_metrics| { broker_metrics.topics.get("__TOTAL__").cloned() })
        .map(|r| format!("{:.1} msg/s", r.1))
        .unwrap_or("no data".to_string());
    let broker_link = format!("/clusters/{}/broker/{}/", cluster_id, broker.id);
    html! {
        tr {
            td a href=(broker_link) (broker.id)
            td (broker.hostname)
            td (byte_rate)
            td (msg_rate)
        }
    }
}

fn broker_table(cluster_id: &str, metadata: &Metadata, metrics: &MetricsCache) -> PreEscaped<String> {
    layout::datatable(html! { tr { th "Broker id" th "Hostname"
        th data-toggle="tooltip" data-container="body"
            title="Total average over the last 15 minutes" "Total byte rate"
        th data-toggle="tooltip" data-container="body"
            title="Total average over the last 15 minutes" "Total msg rate"
        } },
        html! { @for broker in &metadata.brokers {
                    (broker_table_row(cluster_id, broker, metrics))
                }

    })
}

fn topic_table_row(cluster_id: &str, name: &str, partitions: &Vec<Partition>, topic_metrics: &HashMap<String, (f64, f64)>) -> PreEscaped<String> {
    let byte_rate = topic_metrics.get(name)
        .map(|r| format!("{:.1} KB/s", (r.0 / 1000f64)))
        .unwrap_or("no data".to_string());
    let msg_rate = topic_metrics.get(name)
        .map(|r| format!("{:.1} msg/s", r.1))
        .unwrap_or("no data".to_string());
    let chart_link = format!("https://app.signalfx.com/#/dashboard/CM0CgE0AgAA?variables%5B%5D=Topic%3Dtopic:{}", name);
    let topic_link = format!("/clusters/{}/topic/{}/", cluster_id, name);
    let errors = partitions.iter().map(|p| (p.id, p.error.clone())).filter(|&(_, ref error)| error.is_some()).collect::<Vec<_>>();
    let err_str = if errors.len() == 0 {
        "None".to_owned()
    } else {
        format!("{:?}", errors)
    };
    html! {
        tr {
            td a href=(topic_link) (name)
            td (partitions.len())
            td (err_str)
            td (byte_rate)
            td (msg_rate)
            td {
                a href=(chart_link) data-toggle="tooltip" data-container="body"
                    title="Topic chart" {
                    i class="fa fa-bar-chart" {}
                }
            }
        }
    }
}

fn topic_table(cluster_id: &str, metadata: &Metadata, topic_metrics: &HashMap<String, (f64, f64)>) -> PreEscaped<String> {
    layout::datatable(html! { tr { th "Topic name" th "#Partitions" th "Errors"
        th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
        th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
        th "More"} },
        html! { @for (topic_name, partitions) in &metadata.topics {
                    (topic_table_row(cluster_id, topic_name, partitions, topic_metrics))
                }

    })
}

pub fn cluster_handler(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();
    let clusters = cache.metadata.keys();

    let metadata = cache.metadata.get(&cluster_id.to_owned());
    if metadata.is_none() {
        let content = layout::notification("danger", html! { i class="fa fa-frown-o fa-3x" "" " The specified cluster doesn't exist." });
        let page_title = format!("Cluster: {}", cluster_id);
        let html = layout::page(&page_title, &clusters, content);
        return Ok(Response::with((status::Ok, html)));
    }

    let metadata = cache.metadata.get(&cluster_id.to_string()).unwrap();
    let topic_metrics = build_topic_metrics(&cluster_id, &metadata, &cache.metrics);
    let content = html! {
        (layout::panel_right(html! { i class="fa fa-server fa-fw" "" " Brokers" },
            html! { "Last update: " (metadata.refresh_time) },
            broker_table(cluster_id, &metadata, &cache.metrics)))
        (layout::panel_right(html! { i class="fa fa-exchange fa-fw" "" " Topics" },
            html! { "Last update: " (metadata.refresh_time) },
            topic_table(cluster_id, &metadata, &topic_metrics)))
    };
    let html = layout::page(&format!("Cluster: {}", cluster_id), &clusters, content);

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
