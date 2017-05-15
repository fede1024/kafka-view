use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use maud::{PreEscaped, Markup};
use router::Router;

use web_server::pages;
use web_server::server::{CacheType, ConfigArc};
use web_server::view::layout;
use metadata::{Broker, ClusterId};

use cache::Cache;
use config::Config;

use rocket::State;


fn broker_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/brokers", cluster_id);
    layout::datatable_ajax("brokers-ajax", &api_url, cluster_id.name(),
        html! { tr { th "Broker id" th "Hostname"
            th data-toggle="tooltip" data-container="body"
                title="Total average over the last 15 minutes" "Total byte rate"
            th data-toggle="tooltip" data-container="body"
                title="Total average over the last 15 minutes" "Total msg rate"
            }
        }
    )
}

fn topic_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topics", cluster_id);
    layout::datatable_ajax("topics-ajax", &api_url, cluster_id.name(),
               html! { tr { th "Topic name" th "#Partitions" th "Status"
                     th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
                     th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
                   }
              },
    )
}

fn groups_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/groups", cluster_id);
    layout::datatable_ajax("groups-ajax", &api_url, cluster_id.name(),
        html! { tr { th "Group name" th "Status" th "Registered members" th "Stored topic offsets" } },
    )
}

#[get("/clusters/<cluster_id>")]
pub fn cluster_page(cluster_id: ClusterId, cache: State<Cache>, config: State<Config>) -> Markup {
    if cache.brokers.get(&cluster_id).is_none() {
        return pages::warning_page2(
            &format!("Cluster: {}", cluster_id),
            "The specified cluster doesn't exist.")
    }

    let cluster_config = config.clusters.get(&cluster_id);
    let content = html! {
        h3 style="margin-top: 0px" "Information"
        dl class="dl-horizontal" {
            dt "Cluster name: " dd (cluster_id.name())
            @if cluster_config.is_some() {
                dt "Bootstrap list: " dd (cluster_config.unwrap().broker_list.join(", "))
                dt "Zookeeper: " dd (cluster_config.unwrap().zookeeper)
            } @else {
                dt "Bootstrap list: " dd "Cluster configuration is missing"
                dt "Zookeeper: " dd "Cluster configuration is missing"
            }
        }
        h3 "Brokers"
        div (broker_table(&cluster_id))
        h3 "Topics"
        (topic_table(&cluster_id))
        h3 "Consumer groups"
        (groups_table(&cluster_id))
    };
    layout::page2(&format!("Cluster: {}", cluster_id), content)
}
