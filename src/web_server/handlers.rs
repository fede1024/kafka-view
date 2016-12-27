use iron::Plugin;
use iron::prelude::{Request, Response};
use iron::{IronResult, status};
use staticfile::Static;
use mount;
use maud::PreEscaped;
use iron::headers::ContentType;
use iron::modifier::{Modifier, Set};
// use iron::modifiers::Header;

use std::path::Path;
use std::sync::Arc;

use web_server::server::MetadataCache;
use metadata::Metadata;


fn page(content: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        html {
            head (header())
            body (content)
        }
    }
}

fn header() -> PreEscaped<String> {
    html! {
        (PreEscaped("<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\" integrity=\"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u\" crossorigin=\"anonymous\">
            <link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css\" integrity=\"sha384-rHyoN1iRsVXV4nD0JutlnGaslCJuC7uwjduW9SVrLvRYooPp2bWYgmgJQIXwl/Sp\" crossorigin=\"anonymous\">
            <script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js\" integrity=\"sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa\" crossorigin=\"anonymous\"></script>"))
    }
}

fn format_broker_list(brokers: &Vec<i32>) -> String {
    let mut res = "[".to_string();
    res += &brokers.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ");
    res += "]";
    res
}

fn format_metadata(cluster_id: &str, metadata: Arc<Metadata>) -> PreEscaped<String> {
    html! {
        h1 { "Metadata for " (cluster_id) }
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
    let metadata_cache = req.extensions.get::<MetadataCache>().unwrap();

    let mut content = "".to_string();
    for cluster_id in metadata_cache.keys() {
        let metadata = metadata_cache.get(&cluster_id.to_string()).unwrap();
        content += &format_metadata(&cluster_id, metadata).into_string();
    }

    let html = page(PreEscaped(content));

    let mut resp = Response::with((status::Ok, html));
    // resp.set_mut(Header(ContentType::html()));
    Ok(resp)
}

pub struct AssetsHandler;

impl AssetsHandler {
    pub fn new(prefix: &str, mount_path: &str) -> mount::Mount {
        let mut assets_mount = mount::Mount::new();
        assets_mount.mount(prefix, Static::new(Path::new(mount_path)));
        assets_mount
    }
}
