use iron::Plugin;
use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use staticfile::Static;
use persistent::State;
use mount;
use maud;

use std::path::Path;

use web_server::server::MetadataCache;
use metadata::Metadata;
use cache::{Cache, ReplicatedCache};


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
    let metadata_cache = req.get::<State<MetadataCache>>().unwrap();
    let cluster_id = "local_cluster";
    let metadata = {
        match metadata_cache.read() {
            Ok(metadata) => (*metadata).get(&cluster_id.to_string()).unwrap(),
            Err(_) => panic!("Poison error"),
        }
    };

    let markup = html! {
        h1 { "Metadata for " (cluster_id) }
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
