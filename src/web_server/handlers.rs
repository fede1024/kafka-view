use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use staticfile::Static;
use mount;
use maud::PreEscaped;

use std::path::Path;
use std::sync::Arc;

use web_server::server::CacheType;
use web_server::view::layout;
use metadata::Metadata;


fn format_broker_list(brokers: &Vec<i32>) -> String {
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(cluster_id: &str, metadata: Arc<Metadata>) -> PreEscaped<String> {
    html! {
        h2 { "Metadata for " (cluster_id) }
        p { "Last update: " (metadata.refresh_time()) }
        ol {
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
}

pub fn home_handler(req: &mut Request) -> IronResult<Response> {
    let cache = req.extensions.get::<CacheType>().unwrap();

    let mut content = "".to_string();
    for cluster_id in cache.metadata.keys() {
        let metadata = cache.metadata.get(&cluster_id.to_string()).unwrap();
        content += &format_metadata(&cluster_id, metadata).into_string();
    }

    let html = layout::page("Clusters", PreEscaped(content));

    Ok(Response::with((status::Ok, html)))
}

pub struct AssetsHandler;

impl AssetsHandler {
    pub fn new(prefix: &str, mount_path: &str) -> mount::Mount {
        let mut assets_mount = mount::Mount::new();
        assets_mount.mount(prefix, Static::new(Path::new(mount_path)));
        assets_mount
    }
}
