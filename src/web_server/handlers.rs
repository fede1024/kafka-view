use iron::Plugin;
use iron::prelude::{Request, Response};
use iron::{IronResult, Set, status};
use staticfile::Static;
use urlencoded::UrlEncodedBody;
use persistent::State;
use mount;
use maud;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use web_server::server::Fetcher;
use metadata::Metadata;

// pub fn new_task_handler(req: &mut Request) -> IronResult<Response> {
//     println!("Reached the new task handler");
//     println!("req = {:?}", req);
//     let empty_hm = HashMap::new();
//     let hm = match req.get_ref::<UrlEncodedBody>() {
//         Ok(hashmap) => hashmap,
//         Err(ref e) => {
//             println!("{:?}", e);
//             &empty_hm
//         }
//     };
//     println!("encoded data = {:?}", hm);
//     let mut resp = Response::new();
//     resp.set_mut(hbi::Template::new("home", make_test_records())).set_mut(status::Ok);
//     Ok(resp)
// }


// fn render_topic(topic: &str, partitions: &Vec<Partition>) {
//
// }


fn format_broker_list(brokers: &Vec<i32>) -> String {
    // TODO: make faster
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(metadata: &Metadata) -> maud::PreEscaped<String> {
    html! {
        @for (name, partitions) in metadata.topics() {
            li (name)
            ul {
                @for partition in partitions {
                    li { (partition.id) " - " (partition.leader) " " (format_broker_list(&partition.isr)) }
                }
            }
        }
    }
}

pub fn home_handler(req: &mut Request) -> IronResult<Response> {
    let fetcher_lock = req.get::<State<Fetcher>>().unwrap();
    let cluster_name = "local_cluster";
    let metadata = {
        let mut fetcher = fetcher_lock.read().unwrap();
        fetcher.get_metadata(&cluster_name.to_string()).unwrap()
    };

    let markup = html! {
        h1 "Metadata for " cluster_name
        p { "Last update: " (metadata.refresh_time()) }
        ol {
            (format_metadata(&metadata))
        }
    };

    Ok((Response::with((status::Ok, markup))))
}

pub struct AssetsHandler;

impl AssetsHandler {
    pub fn new(prefix: &str, mount_path: &str) -> mount::Mount {
        let mut assets_mount = mount::Mount::new();
        assets_mount.mount(prefix, Static::new(Path::new(mount_path)));
        assets_mount
    }
}
