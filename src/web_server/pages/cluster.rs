use chrono::UTC;
use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use itertools::Itertools;
use maud::PreEscaped;
use router::Router;

use std::collections::HashMap;

use web_server::pages;
use web_server::server::{CacheType, ConfigArc, RequestTimer};
use web_server::view::layout;
use metadata::{Group, Broker, Partition};
use cache::{MetricsCache, Cache};
use offsets::OffsetStore;


fn broker_table(cluster_id: &str, brokers: &Vec<Broker>, metrics: &MetricsCache) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/brokers", cluster_id);
    layout::datatable_ajax(true, "brokers-ajax", &api_url, &cluster_id,
        html! { tr { th "Broker id" th "Hostname"
            th data-toggle="tooltip" data-container="body"
                title="Total average over the last 15 minutes" "Total byte rate"
            th data-toggle="tooltip" data-container="body"
                title="Total average over the last 15 minutes" "Total msg rate"
            }
        }
    )
}

fn topic_table(cluster_id: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topics", cluster_id);
    layout::datatable_ajax(true, "topics-ajax", &api_url, &cluster_id,
               html! { tr { th "Topic name" th "#Partitions" th "Status"
                     th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
                     th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
                   }
              },
    )
}

fn groups_table(cluster_id: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/groups", &cluster_id);
    layout::datatable_ajax(true, "groups-ajax", &api_url, cluster_id,
        html! { tr { th "Group name" th "#Members" th "Status" th "#Topics offsets" } },
    )
}

pub fn cluster_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let ref config = req.extensions.get::<ConfigArc>().unwrap().config;
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let brokers = cache.brokers.get(&cluster_id.to_owned());
    if brokers.is_none() {  // TODO: Improve here
        return pages::warning_page(req,
            &format!("Cluster: {}", cluster_id),
            "The specified cluster doesn't exist.")
    }

    let brokers = brokers.unwrap();
    let topics = cache.topics.filter_clone(|&(ref c, _), _| c == cluster_id);
    let cluster_config = config.clusters.get(cluster_id);
    let content = html! {
        h2 style="margin-top: 0px" "General:"
        dl class="dl-horizontal" {
            dt "Cluster name: " dd (cluster_id)
            @if cluster_config.is_some() {
                dt "Bootstrap list: " dd (cluster_config.unwrap().broker_list.join(", "))
                dt "Zookeeper: " dd (cluster_config.unwrap().zookeeper)
            } @else {
                dt "Bootstrap list: " dd "Cluster configuration is missing"
                dt "Zookeeper: " dd "Cluster configuration is missing"
            }
        }
        h3 "Brokers"
        div (broker_table(cluster_id, &brokers, &cache.metrics))
        h3 "Topics"
        (topic_table(cluster_id))
        h3 "Groups"
        (groups_table(cluster_id))
    };
    let html = layout::page(req, &format!("Cluster: {}", cluster_id), content);

    Ok(Response::with((status::Ok, html)))
}
