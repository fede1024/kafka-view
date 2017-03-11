use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use maud::PreEscaped;
use router::Router;

use web_server::pages;
use web_server::server::{CacheType, ConfigArc};
use web_server::view::layout;
use metadata::Broker;


fn broker_table(cluster_id: &str) -> PreEscaped<String> {
    let api_url = format!("/api/cluster/{}/brokers", cluster_id);
    layout::datatable_ajax("brokers-ajax", &api_url, &cluster_id,
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
    let api_url = format!("/api/cluster/{}/topics", cluster_id);
    layout::datatable_ajax("topics-ajax", &api_url, &cluster_id,
               html! { tr { th "Topic name" th "#Partitions" th "Status"
                     th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
                     th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
                   }
              },
    )
}

fn groups_table(cluster_id: &str) -> PreEscaped<String> {
    let api_url = format!("/api/cluster/{}/groups", &cluster_id);
    layout::datatable_ajax("groups-ajax", &api_url, cluster_id,
        html! { tr { th "Group name" th "Status" th "Registered members" th "Stored topic offsets" } },
    )
}

pub fn cluster_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let ref config = req.extensions.get::<ConfigArc>().unwrap().config;
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    if cache.brokers.get(cluster_id).is_none() {
        return pages::warning_page(req,
            &format!("Cluster: {}", cluster_id),
            "The specified cluster doesn't exist.")
    }

    let cluster_config = config.clusters.get(cluster_id);
    let content = html! {
        h3 style="margin-top: 0px" "Cluster information"
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
        div (broker_table(cluster_id))
        h3 "Topics"
        (topic_table(cluster_id))
        h3 "Consumer groups"
        (groups_table(cluster_id))
    };
    let html = layout::page(req, &format!("{}", cluster_id), content);

    Ok(Response::with((status::Ok, html)))
}
