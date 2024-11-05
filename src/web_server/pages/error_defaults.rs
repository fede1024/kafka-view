use crate::web_server::view::layout;

use maud::{html, Markup};

pub fn warning_page(title: &str, message: &str) -> Markup {
    let content = layout::notification(
        "warning",
        html! {
            div class="flex-container" {
                span class="flex-item" style="padding: 0.3in; font-size: 16pt" {
                    i class="fa fa-frown-o fa-3x" style="vertical-align: middle;" { "" }
                    " " (message)
                }
            }
        },
    );
    layout::page(title, content)
}
