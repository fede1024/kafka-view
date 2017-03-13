use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use maud::PreEscaped;

use web_server::server::CacheType;
use web_server::view::layout;
use cache::{BrokerCache, TopicCache};
use metadata::ClusterId;


fn cluster_pane_layout(cluster_id: &ClusterId, brokers: usize, topics: usize) -> PreEscaped<String> {
    let link = format!("/cluster/{}/", cluster_id.name());
    html! {
        div class="col-lg-4 col-md-6" {
            div class="panel panel-primary" {
                div class="panel-heading" {
                    div class="row" {
                        div class="col-xs-3" i class="fa fa-server fa-5x" {}
                        div class="col-xs-9 text-right" {
                            div style="font-size: 24px" {
                                a href=(link) style="color: inherit; text-decoration: inherit;" (cluster_id.name())
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

fn cluster_pane(cluster_id: &ClusterId, broker_cache: &BrokerCache, topic_cache: &TopicCache) -> PreEscaped<String> {
    let broker_count = broker_cache.get(cluster_id).unwrap_or(Vec::new()).len();
    let topics_count = topic_cache.count(|&(ref c, _)| c == cluster_id);
    cluster_pane_layout(cluster_id, broker_count, topics_count)
}

pub fn clusters_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let mut clusters = cache.brokers.keys();
    clusters.sort();

    let content = html! {
        @for cluster_name in clusters {
			(cluster_pane(&cluster_name, &cache.brokers, &cache.topics))
		}
    };

    let html = layout::page(req, "Clusters", content);

    Ok(Response::with((status::Ok, html)))
}
