use maud::{html, Markup, PreEscaped};
use rocket::State;

use crate::cache::{BrokerCache, Cache, TopicCache};
use crate::metadata::ClusterId;
use crate::web_server::view::layout;

fn cluster_pane_layout(
    cluster_id: &ClusterId,
    brokers: usize,
    topics: usize,
) -> PreEscaped<String> {
    let link = format!("/clusters/{}/", cluster_id.name());
    html! {
        div class="col-lg-4 col-md-6" {
            div class="panel panel-primary" {
                div class="panel-heading" {
                    div class="row" {
                        // div class="col-xs-3" i class="fa fa-server fa-5x" {}
                        div class="col-xs-3" { img style="height: 64px" src="/public/images/kafka_logo_white.png" {} }
                        div class="col-xs-9 text-right" {
                            div style="font-size: 24px" {
                                a href=(link) style="color: inherit; text-decoration: inherit;" { (cluster_id.name()) }
                            }
                            div { (brokers) " brokers" }
                            div { (topics) " topics" }
                        }
                    }
                }
                a href=(link) {
                    div class="panel-footer" {
                        span class="pull-left" { "View Details" }
                        span class="pull-right" { i class="fa fa-arrow-circle-right" {} }
                        div class="clearfix" {}
                    }
                }
            }
        }
    }
}

fn cluster_pane(
    cluster_id: &ClusterId,
    broker_cache: &BrokerCache,
    topic_cache: &TopicCache,
) -> PreEscaped<String> {
    let broker_count = broker_cache.get(cluster_id).unwrap_or_default().len();
    let topics_count = topic_cache.count(|&(ref c, _)| c == cluster_id);
    cluster_pane_layout(cluster_id, broker_count, topics_count)
}

#[get("/clusters")]
pub fn clusters_page(cache: &State<Cache>) -> Markup {
    let mut cluster_ids = cache.brokers.keys();
    cluster_ids.sort();

    let content = html! {
        @for cluster_id in &cluster_ids {
            (cluster_pane(cluster_id, &cache.brokers, &cache.topics))
        }
    };

    layout::page("Clusters", content)
}
