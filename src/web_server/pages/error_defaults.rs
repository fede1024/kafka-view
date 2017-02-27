use web_server::server::RequestTimer;
use web_server::view::layout;
use maud::PreEscaped;
use iron::status;
use iron::prelude::*;

pub fn warning_page(req: &Request, title: &str, message: &str) -> IronResult<Response> {
    let content = layout::notification("warning",
        html! {
            div class="flex-container" {
                span class="flex-item" style="padding: 0.3in; font-size: 16pt" {
                    i class="fa fa-frown-o fa-3x" style="vertical-align: middle;" ""
                    " " (message)
                }
            }
        });
    let html = layout::page(req, title, content);
    Ok(Response::with((status::NotFound, html)))   // TODO fix return status
}

pub fn not_found_page(req: &Request) -> IronResult<Response> {
    warning_page(req, "Page not found", "The page you are looking for doesn't exist.")
}

pub fn todo(req: &mut Request) -> IronResult<Response> {
    let content = html! {
        img src="https://media.giphy.com/media/13HBDT4QSTpveU/giphy.gif"
            style="float:left;max-width:100pt;max-height:100pt"
        span style="padding: 0.3in; font-size: 16pt" {
            "Sorry, this feature is not implemented yet!"
        }
    };
    let html = layout::page(req, "It's in my todo list", content);
    Ok(Response::with((status::NotFound, html)))
}
