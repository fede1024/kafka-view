use maud::{html, Markup, PreEscaped};
use rand::random;

use crate::cache::Cache;
use crate::config::Config;
use crate::metadata::ClusterId;
use crate::web_server::pages;
use crate::web_server::view::layout;

use rocket::State;

fn topic_table(cluster_id: &ClusterId, topic_name: &str) -> PreEscaped<String> {
    let api_url = format!(
        "/api/clusters/{}/topics/{}/topology",
        cluster_id, topic_name
    );
    layout::datatable_ajax(
        "topology-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Id" } th { "Size" } th { "Leader" } th { "Replicas" } th { "ISR" } th { "Status" } } },
    )
}

fn consumer_groups_table(cluster_id: &ClusterId, topic_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/topics/{}/groups", cluster_id, topic_name);
    layout::datatable_ajax(
        "groups-ajax",
        &api_url,
        cluster_id.name(),
        html! { tr { th { "Group name" } th { "Status" } th { "Registered members" } th { "Stored topic offsets" } } },
    )
}

fn graph_link(graph_url: &str, topic: &str) -> PreEscaped<String> {
    let url = graph_url.replace("{%s}", topic);
    html! {
        a href=(url) { "link" }
    }
}

fn topic_tailer_panel(cluster_id: &ClusterId, topic: &str, tailer_id: u64) -> PreEscaped<String> {
    let panel_head = html! {
        i class="fa fa-align-left fa-fw" {} "Messages"
    };
    let panel_body = html! {
        div class="topic_tailer" data-cluster=(cluster_id) data-topic=(topic) data-tailer=(tailer_id) {
            "Tailing recent messages..."
        }
    };
    layout::panel(panel_head, panel_body)
}

#[get("/clusters/<cluster_id>/topics/<topic_name>")]
pub fn topic_page<'a>(
    cluster_id: ClusterId,
    topic_name: &'a str,
    cache: &State<Cache>,
    config: &State<Config>,
) -> Markup {
    let partitions = match cache
        .topics
        .get(&(cluster_id.clone(), topic_name.to_string()))
    {
        Some(partitions) => partitions,
        None => {
            return pages::warning_page(
                &format!("Topic: {}", cluster_id),
                "The specified cluster doesn't exist.",
            )
        }
    };

    let cluster_config = config.clusters.get(&cluster_id).unwrap();
    let _ = cache
        .brokers
        .get(&cluster_id)
        .expect("Cluster should exist"); // TODO: handle better

    let metrics = cache
        .metrics
        .get(&(cluster_id.clone(), topic_name.to_string()))
        .unwrap_or_default()
        .aggregate_broker_metrics();

    let cluster_link = format!("/clusters/{}/", cluster_id.name());
    let content = html! {
        h3 style="margin-top: 0px" {"General information"}
        dl class="dl-horizontal" {
            dt { "Cluster name " dd { a href=(cluster_link) { (cluster_id) } } }
            dt { "Topic name " dd { (topic_name) } }
            dt { "Number of partitions " dd { (partitions.len()) } }
            dt { "Number of replicas " dd { (partitions[0].replicas.len()) } }
            dt { "Traffic last 15 minutes" }
            dd { ( format!("{:.1}   KB/s {:.0} msg/s", metrics.b_rate_15 / 1000f64, metrics.m_rate_15)) }
            @if cluster_config.graph_url.is_some() {
                dt { "Traffic chart" } dd { (graph_link(cluster_config.graph_url.as_ref().unwrap(), topic_name)) }
            }
        }
        h3 { "Topology" }
        (topic_table(&cluster_id, topic_name))
        h3 {"Consumer groups"}
        (consumer_groups_table(&cluster_id, topic_name))
        h3 { "Tailer" }
        @if cluster_config.enable_tailing {
            (topic_tailer_panel(&cluster_id, topic_name, random::<u64>()))
        } @else {
            p { "Topic tailing is disabled in this cluster." }
        }
    };

    layout::page(&format!("Topic: {}", topic_name), content)
}
