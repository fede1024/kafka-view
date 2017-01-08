use web_server::view::layout;
use maud::PreEscaped;
use iron::status;
use iron::prelude::*;


pub fn not_found_page_root(req: &mut Request) -> IronResult<Response> {
    let content = layout::notification("warning",
        html! {
            div class="flex-container" {
                span class="flex-item" style="padding: 0.3in; font-size: 16pt" {
                    i class="fa fa-frown-o fa-3x" style="vertical-align: middle;" ""
                    " The page you are looking for doesn't exist."
                }
            }
        });
    let html = layout::page(&"Page not found", content);
    Ok(Response::with((status::NotFound, html)))
}
