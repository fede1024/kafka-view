use web_server::view::layout;
use iron::status;
use iron::prelude::*;

use maud::Markup;

pub fn warning_page(title: &str, message: &str) -> Markup {
    let content = layout::notification("warning",
                                       html! {
            div class="flex-container" {
                span class="flex-item" style="padding: 0.3in; font-size: 16pt" {
                    i class="fa fa-frown-o fa-3x" style="vertical-align: middle;" ""
                    " " (message)
                }
            }
        });
    layout::page(title, content)
}

pub fn not_found_page() -> Markup {
    // TODO: return 404
    warning_page("Page not found", "The page you are looking for doesn't exist.")
}

pub fn todo() -> Markup {
    let content = html! {
        img src="https://media.giphy.com/media/13HBDT4QSTpveU/giphy.gif"
            style="float:left;max-width:100pt;max-height:100pt"
        span style="padding: 0.3in; font-size: 16pt" {
            "It's in my todo list"
        }
    };
    layout::page("This feature is not implemented yet!", content)
}
