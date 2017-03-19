use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use maud::PreEscaped;

use web_server::server::CacheType;
use web_server::view::layout;
use cache::{BrokerCache, TopicCache};
use metadata::ClusterId;

// use std::collections::BTreeMap;


fn cluster_pane_layout(cluster_id: &ClusterId, brokers: usize, topics: usize) -> PreEscaped<String> {
    let link = format!("/cluster/{}/", cluster_id.name());
    html! {
        div class="col-lg-4 col-md-6" {
            div class="panel panel-primary" {
                div class="panel-heading" {
                    div class="row" {
                        // div class="col-xs-3" i class="fa fa-server fa-5x" {}
                        div class="col-xs-3" img style="height: 64px" src="/public/images/kafka_logo_white.png" {}
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
    let mut cluster_ids = cache.brokers.keys();
    cluster_ids.sort();

//    let mut cluster_groups = BTreeMap::new();
//    let mut generic_clusters = Vec::new();
//
//    for cluster_id in &cluster_ids {
//        let parts = cluster_id.name().splitn(2, ".").collect::<Vec<_>>();
//        if parts.len() == 2 {
//            let group = format!("Cluster type: {}", parts[0]);
//            let list = cluster_groups.entry(group).or_insert(Vec::new());
//            (*list).push((parts[1], cluster_id));
//        } else {
//            generic_clusters.push(cluster_id);
//        }
//    }

//    let content = html! {
//        // h3 style="margin-top: 0px" "Generic clusters:"
//        div class="row" {
//            @for cluster_id in generic_clusters {
//                (cluster_pane(cluster_id, &cache.brokers, &cache.topics))
//            }
//        }
//        @for (group_name, clusters) in cluster_groups {
//            div class="row" {
//                h3 style="margin-top: 0px" (group_name)
//                @for (cluster_name, cluster_id) in clusters {
//                    (cluster_pane(cluster_id, &cache.brokers, &cache.topics))
//                }
//            }
//	 	}
//    };

    let content = html! {
        @for cluster_id in &cluster_ids {
            (cluster_pane(cluster_id, &cache.brokers, &cache.topics))
        }
    };

    let html = layout::page(req, "Clusters", content);

    Ok(Response::with((status::Ok, html)))
}
