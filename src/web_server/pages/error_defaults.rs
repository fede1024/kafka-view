use web_server::server::RequestTimer;
use web_server::view::layout;
use maud::PreEscaped;
use iron::status;
use iron::prelude::*;

pub fn warning_page(req: &Request, title: &str, message: &str) -> IronResult<Response> {
    let &(request_id, _) = req.extensions.get::<RequestTimer>().unwrap();
    let content = layout::notification("warning",
        html! {
            div class="flex-container" {
                span class="flex-item" style="padding: 0.3in; font-size: 16pt" {
                    i class="fa fa-frown-o fa-3x" style="vertical-align: middle;" ""
                    " " (message)
                }
            }
        });
    let html = layout::page(request_id, title, content);
    Ok(Response::with((status::NotFound, html)))   // TODO fix return status
}

pub fn not_found_page(req: &Request) -> IronResult<Response> {
    warning_page(req, "Page not found", "The page you are looking for doesn't exist.")
}
