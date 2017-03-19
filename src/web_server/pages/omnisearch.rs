use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};
use maud::PreEscaped;
use params::{Params, Value};
use iron::prelude::*;

use web_server::server::CacheType;
use web_server::view::layout;
use web_server::pages;
use metadata::ClusterId;


pub fn topic_search(req: &mut Request) -> IronResult<Response> {
    let parameters = req.get_ref::<Params>().unwrap().clone();
    let cache = req.extensions.get::<CacheType>().unwrap();

    let search_string = match parameters.get("search") {
        Some(&Value::String(ref value)) => value,
        _ => "",
    };
    let regex = match parameters.get("regex") {
        Some(&Value::String(ref value)) => value == "on",
        _ => false,
    };

    let search_form = layout::search_form("/topics", "Topic name", search_string, regex);
    let api_url = format!("/api/search/topic?search={}&regex={}", search_string, regex);
    let results = layout::datatable_ajax("topic-search-ajax", &api_url, "",
        html! { tr { th "Cluster name" th "Topic name" th "#Partitions" th "Status"
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
        }}
    );

    let page = layout::page(req, "Topic search", html! {
        (search_form)
        @if search_string.len() > 0 {
            h3 "Search results"
            (results)
        }
    });

    Ok(Response::with((status::Ok, page)))
}
