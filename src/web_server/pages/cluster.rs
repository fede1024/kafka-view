use maud::{html, Markup, PreEscaped};

use crate::metadata::{BrokerId, ClusterId};
use crate::web_server::pages;
use crate::web_server::view::layout;

use crate::cache::Cache;
use crate::config::Config;

use rocket::State;

fn broker_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/brokers", cluster_id);
    layout::datatable_ajax(
        "brokers-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Broker id" } th { "Hostname" }
            th data-toggle="tooltip" data-container="body"
                title="Total average over the last 15 minutes" { "Total byte rate" }
            th data-toggle="tooltip" data-container="body"
                title="Total average over the last 15 minutes" { "Total msg rate" }
            }
        },
    )
}

fn topic_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topics", cluster_id);
    layout::datatable_ajax(
        "topics-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Topic name" } th { "#Partitions" } th { "Status" }
               th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" { "Byte rate" }
               th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" { "Msg rate" }
             }
        },
    )
}

fn groups_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/groups", cluster_id);
    layout::datatable_ajax(
        "groups-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Group name" } th { "Status" } th { "Registered members" } th { "Stored topic offsets" } } },
    )
}

fn reassignment_table(cluster_id: &ClusterId) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/reassignment", cluster_id);
    layout::datatable_ajax(
        "reassignment-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Topic" } th { "Partition" } th { "Reassigned replicas" } th { "Replica sizes" } } },
    )
}

#[get("/clusters/<cluster_id>")]
pub fn cluster_page(cluster_id: ClusterId, cache: &State<Cache>, config: &State<Config>) -> Markup {
    if cache.brokers.get(&cluster_id).is_none() {
        return pages::warning_page(
            &format!("Cluster: {}", cluster_id),
            "The specified cluster doesn't exist.",
        );
    }

    let cluster_config = config.clusters.get(&cluster_id);
    let content = html! {
        h3 style="margin-top: 0px" { "Information" }
        dl class="dl-horizontal" {
            dt { "Cluster name: " } dd { (cluster_id.name()) }
            @if cluster_config.is_some() {
                dt { "Bootstrap list: " } dd { (cluster_config.unwrap().broker_list.join(", ")) }
                dt { "Zookeeper: " } dd { (cluster_config.unwrap().zookeeper) }
            } @else {
                dt { "Bootstrap list: " } dd { "Cluster configuration is missing" }
                dt { "Zookeeper: " } dd { "Cluster configuration is missing" }
            }
        }
        h3 { "Brokers" }
        div { (broker_table(&cluster_id)) }
        h3 { "Topics" }
        (topic_table(&cluster_id))
        h3 { "Consumer groups" }
        (groups_table(&cluster_id))

        @if cluster_config.map(|c| c.show_zk_reassignments).unwrap_or(false) {
            h3 { "Reassignment" }
            (reassignment_table(&cluster_id))
        }
    };
    layout::page(&format!("Cluster: {}", cluster_id), content)
}

#[get("/clusters/<cluster_id>/brokers/<broker_id>")]
pub fn broker_page(
    cluster_id: ClusterId,
    broker_id: BrokerId,
    cache: &State<Cache>,
    config: &State<Config>,
) -> Markup {
    let broker = cache
        .brokers
        .get(&cluster_id)
        .and_then(|brokers| brokers.iter().find(|b| b.id == broker_id).cloned());
    let cluster_config = config.clusters.get(&cluster_id);

    if broker.is_none() || cluster_config.is_none() {
        return pages::warning_page(
            &format!("Broker: {}", broker_id),
            "The specified broker doesn't exist.",
        );
    }

    let broker = broker.unwrap();
    let metrics = cache
        .metrics
        .get(&(cluster_id.to_owned(), "__TOTAL__".to_owned()))
        .unwrap_or_default()
        .aggregate_broker_metrics();
    let content = html! {
        h3 style="margin-top: 0px" { "Information" }
        dl class="dl-horizontal" {
            dt { "Cluster name: " } dd { (cluster_id.name()) }
            dt { "Bootstrap list: " } dd { (cluster_config.unwrap().broker_list.join(", ")) }
            dt { "Zookeeper: " } dd { (cluster_config.unwrap().zookeeper) }
            dt { "Hostname" } dd { (broker.hostname) }
            dt { "Traffic" } dd { (format!("{:.1} KB/s  {:.0} msg/s", metrics.b_rate_15 / 1000f64, metrics.m_rate_15)) }
        }
    };
    layout::page(&format!("Broker: {}", cluster_id), content)
}
