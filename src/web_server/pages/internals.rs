use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use maud::PreEscaped;
use router::Router;

use web_server::pages;
use web_server::server::{CacheType, ConfigArc};
use web_server::view::layout;
use metadata::{Broker, ClusterId};

fn broker_table() -> PreEscaped<String> {
    layout::datatable_ajax("internals-cache-brokers-ajax", "/api/internals/cache/brokers", "",
        html! { tr { th "Cluster id" th "Broker ids" } }
    )
}

fn metrics_table() -> PreEscaped<String> {
    layout::datatable_ajax("internals-cache-metrics-ajax", "/api/internals/cache/metrics", "",
        html! { tr { th "Cluster id" th "Broker id" th "Topics" } }
    )
}

fn cache_description_table(name: &str, key: &str, value: &str) -> PreEscaped<String> {
    html! {
        table style="margin-top: 10px; margin-bottom: 10px" {
            tr {
                td style="font-weight: bold" "Name:"
                td style="font-family: monospace; padding-left: 20px" (name)
            }
            tr {
                td style="font-weight: bold" "Key:"
                td style="font-family: monospace; padding-left: 20px" (key)
            }
            tr {
                td style="font-weight: bold" "Value:"
                td style="font-family: monospace; padding-left: 20px" (value)
            }
        }
    }
}

pub fn caches_page(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();

    let content = html! {
        h3 style="margin-top: 0px" "Information"
//        dl class="dl-horizontal" {
//            dt "Cluster name: " dd (cluster_id.name())
//            @if cluster_config.is_some() {
//                dt "Bootstrap list: " dd (cluster_config.unwrap().broker_list.join(", "))
//                dt "Zookeeper: " dd (cluster_config.unwrap().zookeeper)
//            } @else {
//                dt "Bootstrap list: " dd "Cluster configuration is missing"
//                dt "Zookeeper: " dd "Cluster configuration is missing"
//            }
//        }
        h3 "Brokers"
        (cache_description_table("BrokerCache", "ClusterId", "Vec<Broker>"))
        div (broker_table())
        h3 "Metrics"
        (cache_description_table("MetricsCache", "(ClusterId, BrokerId)", "BrokerMetrics"))
        div (metrics_table())
    };
    let html = layout::page(req, "Caches", content);

    Ok(Response::with((status::Ok, html)))
}
