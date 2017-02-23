use router;
use iron;

use web_server::pages;
use web_server::handlers;
use web_server::server::RequestTimer;
use router::Router;

use iron::prelude::*;
use iron::modifiers::Redirect;
use iron::{Url, status};


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

pub fn chain() -> iron::Chain {
    let mut router = router::Router::new();
    router.get("/", handlers::home_handler, "get_home");
    //router.get("/", redirect_to_clusters_page, "get_home");
    router.get("/clusters/", pages::clusters_page, "clusters");
    router.get("/clusters/:cluster_id/", pages::cluster_page, "get_cluster");
    router.get("/clusters/:cluster_id/topic/:topic_name/", pages::topic_page, "get_page");
    // router.post("/new_task", handlers::new_task_handler, "new_task");
    router.get("/public/*", handlers::AssetsHandler::new("/public/", "resources/web_server/public/"), "public_assets");
    router.get("/meta/request_time/:request_id/", request_timing, "request_timing");
    iron::Chain::new(router)
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
