use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use maud::PreEscaped;

use web_server::server::{CacheType, RequestTimer};
use web_server::view::layout;
use metadata::Metadata;
use cache::{MetadataCache, MetricsCache};


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

pub fn clusters_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let request_timer = req.extensions.get::<RequestTimer>().unwrap();
    let mut clusters = cache.metadata.keys();
    clusters.sort();

    let content = html! {
        @for cluster_name in clusters {
			(cluster_pane(&cluster_name, &cache.metadata.get(&cluster_name).unwrap()))
		}
    };

    let html = layout::page(request_timer.request_id, "Clusters", content);

    Ok(Response::with((status::Ok, html)))
}
