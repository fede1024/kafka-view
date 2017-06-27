use maud::Markup;
use rocket::request::{FromForm, FormItems};
use rocket::http::uri::URI;

use web_server::view::layout;

#[derive(Debug)]
pub struct OmnisearchFormParams {
    pub string: String,
    pub regex: bool,
}

impl<'f> FromForm<'f> for OmnisearchFormParams {
    type Error = ();

    fn from_form_items(form_items: &mut FormItems<'f>) -> Result<Self, Self::Error> {
        let mut params = OmnisearchFormParams{string: "".to_owned(), regex: false};
        for (key, value) in form_items {
            match key {
                "string" => params.string = URI::percent_decode_lossy(value.as_bytes()).to_string(),
                "regex" => params.regex = value == "on" || value == "true",
                _ => {},
            }
        }
        Ok(params)
    }
}

#[get("/omnisearch")]
pub fn omnisearch() -> Markup {
    omnisearch_p(OmnisearchFormParams{string: "".to_owned(), regex: false})
}

#[get("/omnisearch?<search>")]
pub fn omnisearch_p(search: OmnisearchFormParams) -> Markup {
    let search_form = layout::search_form("/omnisearch", "Omnisearch", &search.string, search.regex);
    let api_url = format!("/api/search/topic?string={}&regex={}", &search.string, search.regex);
    let topics = layout::datatable_ajax("topic-search-ajax", &api_url, "",
        html! { tr { th "Cluster name" th "Topic name" th "#Partitions" th "Status"
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
        }}
    );
    let api_url = format!("/api/search/consumer?string={}&regex={}", &search.string, search.regex);
    let consumers = layout::datatable_ajax("group-search-ajax", &api_url, "",
        html! { tr { th "Cluster" th "Group name" th "Status" th "Registered members" th "Stored topic offsets" } }
    );

    layout::page("Omnisearch", html! {
        (search_form)
        @if search.string.len() > 0 {
            h3 "Topics"
            (topics)
        }
        @if search.string.len() > 0 {
            h3 "Consumers"
            (consumers)
        }
    })
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


#[get("/topics")]
pub fn topic_search() -> Markup {
    topic_search_p(OmnisearchFormParams{string: "".to_owned(), regex: false})
}

#[get("/topics?<search>")]
pub fn topic_search_p(search: OmnisearchFormParams) -> Markup {
    let search_form = layout::search_form("/topics", "Topic name", &search.string, search.regex);
    let api_url = format!("/api/search/topic?string={}&regex={}", &search.string, search.regex);
    let results = layout::datatable_ajax("topic-search-ajax", &api_url, "",
        html! { tr { th "Cluster name" th "Topic name" th "#Partitions" th "Status"
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Byte rate"
             th data-toggle="tooltip" data-container="body" title="Average over the last 15 minutes" "Msg rate"
        }}
    );

    layout::page("Topic search", html! {
        (search_form)
        @if search.string.len() > 0 {
            h3 "Search results"
            (results)
        }
    })
}
