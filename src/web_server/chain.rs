use iron::middleware::Handler;
use iron::modifiers::Redirect;
use iron::prelude::*;
use iron::{IronResult, status};
use iron::modifier::Modifier;
use iron;
use mount;
use router::{Router, url_for};
use staticfile::{Cache, Static};

use web_server::api;
use web_server::pages;
use web_server::server::RequestTimer;

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;


fn request_timing(req: &mut Request) -> IronResult<Response> {
    let request_id = req.extensions.get::<Router>().unwrap().find("request_id").unwrap();
    let request_timer = req.extensions.get::<RequestTimer>().unwrap();

    let time_str = request_id.parse::<i32>().ok()
        .and_then(|r_id| request_timer.timings.lock().expect("Poison error")
                            .iter().find(|&&(req_id, _, _)| req_id == r_id)
                            .map(|&(_, time, _)| format!("{:.3} ms", time as f64 / 1000f64)))
        .unwrap_or("?".to_owned());

    Ok(Response::with((status::Ok, time_str)))
}

// fn redirect_to_clusters_page(req: &mut Request) -> IronResult<Response> {
//     let request_url = req.url.clone().into_generic_url();
//     let new_url = request_url.join("/clusters/").unwrap();
//     Ok(Response::with((status::Found, Redirect(iron::Url::from_generic_url(new_url).unwrap()))))
// }

// fn redirect_to2(dest: &str) -> Box<Fn(&mut Request) -> IronResult<Response>> {
//     let dest_owned = dest.to_owned();
//     Box::new(move |req| {
//         let request_url = req.url.clone().into_generic_url();
//         let new_url = request_url.join(&dest_owned).unwrap();
//         Ok(Response::with((status::Found, Redirect(iron::Url::from_generic_url(new_url).unwrap()))))
//     })
//}

fn redirect_to(dest: &str) -> impl Handler {
    let dest_owned = dest.to_owned();
    move |req: &mut Request| {
        let dest_url = url_for(req, &dest_owned, HashMap::new());
        Ok(Response::with((status::Found, Redirect(dest_url))))
    }
}

pub fn chain() -> iron::Chain {
    let mut router = Router::new();
    router.get("/", redirect_to("clusters"), "home");
    router.get("/clusters", pages::clusters_page, "clusters");
    router.get("/cluster/:cluster_id", pages::cluster_page, "cluster");
    router.get("/cluster/:cluster_id/topic/:topic_name", pages::topic_page, "topic");
    router.get("/cluster/:cluster_id/group/:group_name", pages::group_page, "group");

    // search
    router.get("/topics", pages::topic_search, "topics");
    router.get("/consumers", pages::consumer_search, "consumers");

    // Various
    router.get("/public/*", AssetsHandler::new("/public/", "resources/web_server/public/"), "public_assets");
    router.get("/meta/request_time/:request_id/", request_timing, "request_timing");

    // API
    router.get("/api/cluster/:cluster_id/brokers", api::cluster_brokers, "api_cluster_brokers");
    router.get("/api/cluster/:cluster_id/topics", api::cluster_topics, "api_cluster_topics");
    router.get("/api/cluster/:cluster_id/groups", api::cluster_groups, "api_cluster_groups");
    router.get("/api/cluster/:cluster_id/topic/:topic_name/topology", api::topic_topology, "api_topic_topology");
    router.get("/api/cluster/:cluster_id/topic/:topic_name/groups", api::topic_groups, "api_topic_groups");
    router.get("/api/cluster/:cluster_id/group/:group_name/members", api::group_members, "api_group_members");
    router.get("/api/cluster/:cluster_id/group/:group_name/offsets", api::group_offsets, "api_group_offsets");
    router.get("/api/search/topic", api::topic_search, "api_topic_search");
    router.get("/api/search/consumer", api::consumer_search, "api_consumer_search");

    // todo
    router.get("/brokers", pages::todo, "brokers");
    router.get("/cluster/:cluster_id/broker/:broker_id", pages::todo, "broker");
    iron::Chain::new(router)
}

struct AssetsHandler;

impl AssetsHandler {
    pub fn new(prefix: &str, mount_path: &str) -> mount::Mount {
        let mut assets_mount = mount::Mount::new();
        let mut static_files = Static::new(Path::new(mount_path));
        // static_files.modify(Cache::new(Duration::from_seconds(120)));
        let cache = Cache::new(Duration::from_secs(120));
        cache.modify(&mut static_files);
        assets_mount.mount(prefix, static_files);
        assets_mount
    }
}

#[cfg(test)]
mod chain_tests {

    extern crate router;
    extern crate iron;
    extern crate iron_test;

    use iron::Headers;
    use iron::response::{Response, ResponseBody};
    use iron::status::Status;

    #[test]
    fn get_javascript_responds_correctly() {
        let expected_body = include_str!("public/javascripts/main.js");
        let chain = super::chain();
        let response = iron_test::request::get(
            "http://localhost:3000/public/javascripts/main.js",
            Headers::new(),
            &chain
        ).unwrap();

        assert_eq!(Some(Status::Ok), response.status);
        assert_eq!(expected_body, response_body(response).unwrap());
    }

    #[test]
    fn get_css_resonds_correctly() {
        let expected_body = include_str!("public/stylesheets/main.css");
        let chain = super::chain();
        let response = iron_test::request::get(
            "http://localhost:3000/public/stylesheets/main.css",
            Headers::new(),
            &chain
        ).unwrap();

        assert_eq!(Some(Status::Ok), response.status);
        assert_eq!(expected_body, response_body(response).unwrap());
    }

    // #[test]
    // fn post_new_task_resonds_with_200() {
    //     let chain = super::chain();
    //     let response = iron_test::request::post(
    //         "http://localhost:3000/new_task",
    //         Headers::new(),
    //         "",
    //         &chain
    //     ).unwrap();

    //     assert_eq!(Some(Status::Ok), response.status)
    // }

    #[test]
    fn get_root_responds_with_200() {
        let chain = super::chain();
        let response = iron_test::request::get(
            "http://localhost:3000",
            Headers::new(),
            &chain
        ).unwrap();

        assert_eq!(Some(Status::Ok), response.status)
    }

    fn response_body(response: Response) -> Option<String> {
        if let Some(mut body) = response.body {
            let mut buf = Vec::new();
            body.write_body(&mut ResponseBody::new(&mut buf)).unwrap();
            Some(String::from_utf8(buf).unwrap())
        } else {
            None
        }
    }
}
