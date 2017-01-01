use router;
use iron;

use web_server::handlers;

pub fn chain() -> iron::Chain {
    let mut router = router::Router::new();
    router.get("/", handlers::home_handler, "get_home");
    router.get("/clusters/:cluster_id/", handlers::cluster_handler, "get_cluster");
    // router.post("/new_task", handlers::new_task_handler, "new_task");
    router.get("/public/*", handlers::AssetsHandler::new("/public/", "resources/web_server/public/"), "public_assets");
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
    fn get_javascript_resonds_correctly() {
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
