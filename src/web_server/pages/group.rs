use maud::{Markup, PreEscaped};

use web_server::pages;
use web_server::view::layout;
use metadata::{Broker, ClusterId};
use cache::Cache;

use rocket::State;

fn group_members_table(cluster_id: &ClusterId, group_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/groups/{}/members", cluster_id, group_name);
    layout::datatable_ajax("group-members-ajax", &api_url, cluster_id.name(),
           html! { tr { th "Member id" th "Client id" th "Hostname" } },
    )
}

fn group_offsets_table(cluster_id: &ClusterId, group_name: &str) -> PreEscaped<String> {
    let api_url = format!("/api/clusters/{}/groups/{}/offsets", cluster_id, group_name);
    layout::datatable_ajax("group-offsets-ajax", &api_url, cluster_id.name(),
        html! { tr { th "Topic" th "Partition" th "Size" th "Low mark" th "High mark"
                     th "Current offset" th "Lag" th "Lag %"} },
    )
}

#[get("/clusters/<cluster_id>/groups/<group_name>")]
pub fn group_page(cluster_id: ClusterId, group_name: &str, cache: State<Cache>) -> Markup {
    if cache.brokers.get(&cluster_id).is_none() {
        return pages::warning_page(group_name, "The specified cluster doesn't exist.")
    }

    let group_state = match cache.groups.get(&(cluster_id.to_owned(), group_name.to_owned())) {
        Some(group) => group.state,
        None => "Not registered".to_string(),
    };

    let content = html! {
        h3 style="margin-top: 0px" "Information"
        dl class="dl-horizontal" {
            dt "Cluster name:" dd (cluster_id)
            dt "Group name: " dd (group_name)
            dt "Group state: " dd (group_state)
        }
        h3 "Members"
        div (group_members_table(&cluster_id, group_name))
        h3 "Offsets"
        div (group_offsets_table(&cluster_id, group_name))
    };

    layout::page(&format!("Group: {}", group_name), content)
}

