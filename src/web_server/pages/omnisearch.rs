use iron::prelude::{Request, Response};
use router::Router;
use iron::{IronResult, status};
use maud::{Markup, PreEscaped};
use iron::prelude::*;
use urlencoded::UrlEncodedQuery;
use rocket::State;

use web_server::server::CacheType;
use web_server::view::layout;
use web_server::pages;
use metadata::ClusterId;
use cache::Cache;

use std::collections::HashMap;

#[derive(FromForm, Debug)]
pub struct OmnisearchFormParams {
    string: String,
    regex: bool,
}

#[get("/consumers")]
pub fn consumer_search() -> Markup {
    consumer_search_p(OmnisearchFormParams{string: "".to_owned(), regex: false})
}

#[get("/consumers?<search>")]
pub fn consumer_search_p(search: OmnisearchFormParams) -> Markup {
    let search_form = layout::search_form("/consumers", "Consumer name", &search.string, search.regex);
    let api_url = format!("/api/search/consumer?string={}&regex={}", &search.string, search.regex);
    let results = layout::datatable_ajax("group-search-ajax", &api_url, "",
         html! { tr { th "Cluster" th "Group name" th "Status" th "Registered members" th "Stored topic offsets" } }
    );

    layout::page("Consumer search", html! {
        (search_form)
        @if search.string.len() > 0 {
            h3 "Search results"
            (results)
        }
    })
}


pub fn topic_search() {}

//pub fn topic_search(req: &mut Request) -> IronResult<Response> {
//    let params = req.get_ref::<UrlEncodedQuery>().unwrap_or(&HashMap::new()).clone();
//    let cache = req.extensions.get::<CacheType>().unwrap();
//
//    let search_string = params.get("search")
//        .map(|results| results[0].as_str())
//        .unwrap_or("");
//    let regex = params.get("regex")
//        .map(|results| results[0] == "on")
//        .unwrap_or(false);
//
//    let search_form = layout::search_form("/topics", "Topic name", search_string, regex);
//    let api_url = format!("/api/search/topic?search={}&regex={}", search_string, regex);
//    let results = layout::datatable_ajax("topic-search-ajax", &api_url, "",
//        html! { tr { th "Cluster name" th "Topic name" th "#Partitions" th "Status"
//             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
//             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
//        }}
//    );
//
//    let page = layout::page(req, "Topic search", html! {
//        (search_form)
//        @if search_string.len() > 0 {
//            h3 "Search results"
//            (results)
//        }
//    });
//
//    Ok(Response::with((status::Ok, page)))
//}
