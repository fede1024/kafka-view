use chrono::UTC;
use iron::middleware::Handler;
use iron::modifiers::Redirect;
use iron::prelude::*;
use iron::{IronResult, status};
use iron;
use maud::PreEscaped;
use mount;
use router::{Router, url_for};
use staticfile::Static;

use web_server::pages;
use web_server::server::RequestTimer;

use std::collections::HashMap;
use std::path::Path;


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
        // let request_url = req.url.clone().into_generic_url();
        // let new_url = request_url.join(&dest_owned).unwrap();
        let dest_url = url_for(req, &dest_owned, HashMap::new());
        Ok(Response::with((status::Found, Redirect(dest_url))))
    }
}

pub fn chain() -> iron::Chain {
    let mut router = Router::new();
    router.get("/", redirect_to("clusters"), "home");
    router.get("/clusters/", pages::clusters_page, "clusters");
    router.get("/clusters/:cluster_id/", pages::cluster_page, "cluster");
    router.get("/clusters/:cluster_id/topic/:topic_name/", pages::topic_page, "topic");
    router.get("/public/*", AssetsHandler::new("/public/", "resources/web_server/public/"), "public_assets");
    router.get("/meta/request_time/:request_id/", request_timing, "request_timing");

    // todo
    router.get("/brokers/", pages::todo, "brokers");
    router.get("/topics/", pages::todo, "topics");
    router.get("/consumers/", pages::todo, "consumers");
    router.get("/clusters/:cluster_id/group/:group_id/", pages::todo, "group");
    router.get("/clusters/:cluster_id/consumer_offset/:group_id/", pages::todo, "consumer_offset");
    iron::Chain::new(router)
}

struct AssetsHandler;

impl AssetsHandler {
    pub fn new(prefix: &str, mount_path: &str) -> mount::Mount {
        let mut assets_mount = mount::Mount::new();
        assets_mount.mount(prefix, Static::new(Path::new(mount_path)));
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
