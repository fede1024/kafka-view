use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};
use maud::PreEscaped;

use web_server::server::CacheType;
use web_server::view::layout;
use web_server::pages;
use metadata::{Metadata, Partition};
use cache::{MetadataCache, MetricsCache};

use std::collections::HashMap;


fn cluster_pane_layout(name: &str, brokers: usize, topics: usize) -> PreEscaped<String> {
    let link = format!("/clusters/{}/", name);
    html! {
        div class="col-lg-4 col-md-6" {
            div class="panel panel-primary" {
                div class="panel-heading" {
                    div class="row" {
                        div class="col-xs-3" i class="fa fa-server fa-5x" {}
                        div class="col-xs-9 text-right" {
                            div style="font-size: 24px" {
                                a href=(link) style="color: inherit; text-decoration: inherit;" (name)
                            }
                            div { (brokers) " brokers" }
                            div { (topics) " topics" }
                        }
                    }
                }
                a href=(link) {
                    div class="panel-footer" {
                        span class="pull-left" "View Details"
                        span class="pull-right" i class="fa fa-arrow-circle-right" {}
                        div class="clearfix" {}
                    }
                }
            }
        }
    }
}

fn cluster_pane(name: &str, metadata: &Metadata) -> PreEscaped<String> {
    let broker_count = metadata.brokers.len();
    let topics_count = metadata.topics.len();
    cluster_pane_layout(name, broker_count, topics_count)
}

fn format_broker_list(brokers: &Vec<i32>) -> String {
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(cluster_id: &str, topic_name: &str, partitions: &Vec<Partition>,
        topic_metrics: HashMap<String, (f64, f64)>) -> PreEscaped<String> {

    let content = html! {
        ul {
            @for partition in partitions {
                li { (partition.id) " - " (partition.leader) " " (format_broker_list(&partition.isr)) }
            }
        }
    };
    layout::panel(html!("Topology"), content)
}

pub fn topic_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();
    let topic_name = req.extensions.get::<Router>().unwrap().find("topic_name").unwrap();

    let metadata = match cache.metadata.get(&cluster_id.to_owned()) {
        Some(meta) => meta,
        None => {
            return pages::warning_page(req,
                &format!("Topic: {}", cluster_id),
                "The specified cluster doesn't exist.")
        }
    };

    let partitions = match metadata.topics.get(topic_name) {
        Some(parts) => parts,
        None => {
            return pages::warning_page(req,
                &format!("Topic: {}", cluster_id),
                "The specified cluster doesn't exist.")
        }
    };

    let topic_metrics = pages::cluster::build_topic_metrics(&cluster_id, &metadata, &cache.metrics);
    let content = html! {
        (format_metadata(cluster_id, topic_name, partitions, topic_metrics))
    };

    let html = layout::page(&format!("Topic: {}", topic_name), content);

    Ok(Response::with((status::Ok, html)))
}

