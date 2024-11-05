use maud::{html, Markup, PreEscaped};
use rocket::State;

use crate::cache::Cache;
use crate::web_server::view::layout;

fn broker_table() -> PreEscaped<String> {
    layout::datatable_ajax(
        "internals-cache-brokers-ajax",
        "/api/internals/cache/brokers",
        "",
        html! { tr { th { "Cluster id" } th { "Broker ids" } } },
    )
}

fn metrics_table() -> PreEscaped<String> {
    layout::datatable_ajax(
        "internals-cache-metrics-ajax",
        "/api/internals/cache/metrics",
        "",
        html! { tr { th { "Cluster id" } th { "Topic name" } th { "Brokers" } } },
    )
}

fn offsets_table() -> PreEscaped<String> {
    layout::datatable_ajax(
        "internals-cache-offsets-ajax",
        "/api/internals/cache/offsets",
        "",
        html! { tr { th { "Cluster id" } th { "Consumer group" } th { "Topic name" } th { "Offsets" } } },
    )
}

fn cache_description_table(name: &str, key: &str, value: &str, count: usize) -> PreEscaped<String> {
    html! {
        table style="margin-top: 10px; margin-bottom: 10px" {
            tr {
                td style="font-weight: bold" { "Name:" }
                td style="font-family: monospace; padding-left: 20px" { (name) }
            }
            tr {
                td style="font-weight: bold" { "Key:" }
                td style="font-family: monospace; padding-left: 20px" { (key) }
            }
            tr {
                td style="font-weight: bold" { "Value:" }
                td style="font-family: monospace; padding-left: 20px" { (value) }
            }
            tr {
                td style="font-weight: bold" { "Items count:" }
                td style="font-family: monospace; padding-left: 20px" { (count) }
            }
        }
    }
}

#[get("/internals/caches")]
pub fn caches_page(cache: &State<Cache>) -> Markup {
    let content = html! {
        h3 style="margin-top: 0px" { "Information" }
        h3 { "Brokers" }
        (cache_description_table("BrokerCache", "ClusterId", "Vec<Broker>", cache.brokers.keys().len()))
        div { (broker_table()) }
        h3 { "Metrics" }
        (cache_description_table("MetricsCache", "(ClusterId, TopicName)", "TopicMetrics", cache.metrics.keys().len()))
        div { (metrics_table()) }
        h3 { "Offsets" }
        (cache_description_table("OffsetsCache", "(ClusterId, GroupName, TopicName)", "Vec<i64>", cache.offsets.keys().len()))
        div { (offsets_table()) }
    };
    layout::page("Caches", content)
}

fn live_consumers_table() -> PreEscaped<String> {
    layout::datatable_ajax(
        "internals-live-consumers-ajax",
        "/api/internals/live_consumers",
        "",
        html! { tr { th { "Id" } th { "Cluster id" } th { "Topic name" } th { "Last poll" } } },
    )
}

#[get("/internals/live_consumers")]
pub fn live_consumers_page() -> Markup {
    let content = html! {
        h3 style="margin-top: 0px" { "Active instances" }
        div { (live_consumers_table()) }
    };
    layout::page("Live consumers", content)
}
