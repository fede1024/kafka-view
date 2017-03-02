use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};

use cache::{MetricsCache, Cache};
use web_server::server::{CacheType, ConfigArc, RequestTimer};
use metrics::build_topic_metrics;
use utils::json_response;

//fn topic_table_row(cluster_id: &str, name: &str, partitions: &Vec<Partition>, topic_metrics: &HashMap<String, (f64, f64)>) -> PreEscaped<String> {
//    let rate = topic_metrics.get(name)
//        .map(|r| (format!("{:.1} KB/s", (r.0 / 1000f64)), format!("{:.0} msg/s", r.1)))
//        .unwrap_or(("no data".to_string(), "no data".to_string()));
//    let chart_link = format!("https://app.signalfx.com/#/dashboard/CM0CgE0AgAA?variables%5B%5D=Topic%3Dtopic:{}", name);
//    let topic_link = format!("/clusters/{}/topic/{}/", cluster_id, name);
//    let errors = partitions.iter().map(|p| (p.id, p.error.clone())).filter(|&(_, ref error)| error.is_some()).collect::<Vec<_>>();
//    let err_str = if errors.len() == 0 {
//        html!{ i class="fa fa-check fa-fw" style="color: green" {} }
//    } else {
//        //html!{ i class="fa fa-exclamation-triangle fa-fw" style="color: yellow" {} }
//        html!{ i class="fa fa-times fa-fw" style="color: red" {} }
//    };
//    html! {
//        tr {
//            td a href=(topic_link) (name)
//            td (partitions.len()) td (err_str)
//            td (rate.0) td (rate.1)
//            td {
//                a href=(chart_link) data-toggle="tooltip" data-container="body"
//                    title="Topic chart" {
//                    i class="fa fa-bar-chart" {}
//                }
//            }
//        }
//    }
//}

pub fn cluster_topics(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();
    let cluster_id = req.extensions.get::<Router>().unwrap().find("cluster_id").unwrap();

    let brokers = cache.brokers.get(&cluster_id.to_owned());
    if brokers.is_none() {  // TODO: Improve here
        return Ok(Response::with((status::NotFound, "")));
    }

    let brokers = brokers.unwrap();
    let topics = cache.topics.filter_clone(|&(ref c, _), _| c == cluster_id);
    let topic_metrics = build_topic_metrics(&cluster_id, &brokers, topics.len(), &cache.metrics);

    let mut result_data = Vec::with_capacity(topics.len());
    for &((_, ref topic_name), ref partitions) in topics.iter() {
        let rate = topic_metrics.get(topic_name)
            .map(|r| (format!("{:.1} KB/s", (r.0 / 1000f64)), format!("{:.0} msg/s", r.1)))
            .unwrap_or(("no data".to_string(), "".to_string()));
        let topic_link = format!("/clusters/{}/topic/{}/", cluster_id, topic_name);
        let errors = partitions.iter().map(|p| (p.id, p.error.clone())).filter(|&(_, ref error)| error.is_some()).collect::<Vec<_>>();
        let err_str = if errors.len() == 0 {
            "OK"
        } else {
            "ERR"
        };
        result_data.push(json!((topic_name, partitions.len(), err_str, rate.0, rate.1)));
    }

    let result = json!({"data": result_data});
    Ok(json_response(result))
}

pub fn cluster_brokers(req: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "")))
}

pub fn cluster_groups(req: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "")))
}

pub fn cluster_offsets(req: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "")))
}
