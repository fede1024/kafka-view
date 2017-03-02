use maud::PreEscaped;
use web_server::server::RequestTimer;
use iron::Request;

pub fn format_cluster_path(cluster_id: &str) -> String {
    format!("/clusters/{}/", cluster_id)
}

pub fn notification(n_type: &str, content: PreEscaped<String>) -> PreEscaped<String> {
    let alert_class = format!("alert alert-{}", n_type);
    html! {
        div class=(alert_class) {
            (content)
        }
    }
}

pub fn panel(title: PreEscaped<String>, content: PreEscaped<String>) -> PreEscaped<String> {
    panel_right(title, html!{}, content)
}

pub fn panel_right(title: PreEscaped<String>, right_side: PreEscaped<String>, content: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        div class="panel panel-default" {
            div class="panel-heading" {
                span style="font-weight: bold" (title)
                div class="pull-right" (right_side)
            }
            div class="panel-body" (content)
        }
    }
}

pub fn datatable(loading: bool, id: &str, table_header: PreEscaped<String>, table_body: PreEscaped<String>) -> PreEscaped<String> {
    let table_id = format!("datatable-{}", id);
    let table_style = if loading { "display: none" } else { "" };
    html! {
        @if loading {
            div class="table-loader-marker" style="text-align: center; padding: 0.3in;" {
                div style="display: inline-block" {
                    i class="fa fa-spinner fa-spin fa-4x" {}
                    span class="sr-only" "Loading..."
                }
            }
        }
        table id=(table_id) width="100%" class="table table-striped table-bordered table-hover"
            style=(table_style)
        {
            thead { (table_header) }
            tbody { (table_body) }
        }
    }
}

pub fn datatable_ajax(loading: bool, id: &str, url: &str, table_header: PreEscaped<String>) -> PreEscaped<String> {
    let table_id = format!("datatable-{}", id);
    html! {
        table id=(table_id) data-url=(url) data-cluster-id="scribe.uswest1-devc" width="100%" class="table table-striped table-bordered table-hover" {
            thead { (table_header) }
        }
    }
}

pub fn table<'a, H, R>(headers: H, rows: R) -> PreEscaped<String>
    where H: Iterator<Item=&'a PreEscaped<String>>,
          R: Iterator<Item=&'a Vec<PreEscaped<String>>>
    {
    html! {
        table width="100%" class="table table-striped table-bordered table-hover load-datatable" {
            thead {
                tr {
                    @for header in headers {
                        th (header)
                    }
                }
            }
            tbody {
                @for row in rows {
                    tr class="odd" {
                        @for column in row {
                            td (column)
                        }
                    }
                }
            }
        }
    }
}

fn html_head(title: &str) -> PreEscaped<String> {
    html! {
        head profile="http://www.w3.org/2005/10/profile" {
            link rel="icon" type="image/png" href="/public/images/webkafka_favicon.png" {}
            meta charset="utf-8" {}
            meta http-equiv="X-UA-Compatible" content="IE=edge" {}
            meta name="viewport" content="width=device-width, initial-scale=1" {}
            title (title)
            link href="/public/sb-admin-2/vendor/bootstrap/css/bootstrap.min.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/vendor/metisMenu/metisMenu.min.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/vendor/datatables-plugins/dataTables.bootstrap.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/vendor/datatables-responsive/dataTables.responsive.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/dist/css/sb-admin-2.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/vendor/font-awesome/css/font-awesome.min.css" rel="stylesheet" type="text/css" {}
            link href="/public/my_css.css" rel="stylesheet" type="text/css" {}
        }
    }
}

fn navbar_header() -> PreEscaped<String> {
    html! {
        div class="navbar-header" {
            button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse" {
                span class="sr-only" Toggle navigation {}
                span class="icon-bar" {}
                span class="icon-bar" {}
                span class="icon-bar" {}
            }
            a class="navbar-brand" href="/" {
                    img src="/public/images/kafka_logo.png"
                        style="float:left;max-width:160%;max-height:160%; margin-top: -0.06in; margin-right: 0.07in"
                        align="bottom"
                "Kafka-web"
            }
        }
    }
}

fn navbar_top() -> PreEscaped<String> {
    html! {
        ul class="nav navbar-top-links navbar-right" {
            li class="dropdown" {
                a class="dropdown-toggle" data-toggle="dropdown" href="#" {
                    i class="fa fa-gear fa-fw" {}
                    i class="fa fa-caret-down" {}
                }
                ul class="dropdown-menu dropdown-user" {
                    li { a href="#" {i class="fa fa-user fa-fw" {} "User Profile" } }
                    li { a href="#" {i class="fa fa-gear fa-fw" {} "Settings" } }
                    li class="divider" {}
                    li { a href="#" {i class="fa fa-sign-out fa-fw" {} "Logout" } }
                }
            }
        }
    }
}

fn navbar_side() -> PreEscaped<String> {
    html! {
        div class="navbar-default sidebar" role="navigation" {
            div class="sidebar-nav navbar-collapse" {
                ul class="nav" id="side-menu" {
                    li class="sidebar-search" {
                        div class="input-group custom-search-form" {
                            input type="text" class="form-control" placeholder="Search..."
                            span class="input-group-btn" {
                                button class="btn btn-default" type="button" {
                                    i class="fa fa-search" {}
                                }
                            }
                        }
                    }
                    // li a href="/" { i class="fa fa-dashboard fa-fw" {}  " Home" }
                    li a href="/" style="font-size: 12pt" { i class="fa fa-info-circle fa-fw" {}  " Home" }
                    li a href="/clusters/" style="font-size: 12pt" { i class="fa fa-server fa-fw" {}  " Clusters" }
                    li a href="/topics/" style="font-size: 12pt" { i class="fa fa-exchange fa-fw" {}  " Topics" }
                    li a href="/consumers/" style="font-size: 12pt" { i class="fa fa-cubes fa-fw" {}  " Consumers" }
                    //li {
                    //    a href="#" {
                    //        i class="fa fa-server fa-fw" {} " Clusters"
                    //        span class="fa arrow" {}
                    //    }
                    //    ul class="nav nav-second-level" {
                    //        @for cluster_id in clusters.iter() {
                    //            li a href=(format_cluster_path(cluster_id)) (cluster_id)
                    //        }
                    //        // li {
                    //        //     a href="#" { "Third Level" span class="fa arrow" {} }
                    //        //     ul class="nav nav-third-level" {
                    //        //         li a href="#" "Third Level Item"
                    //        //         li a href="#" "Third Level Item"
                    //        //         li a href="#" "Third Level Item"
                    //        //         li a href="#" "Third Level Item"
                    //        //     }
                    //        // }
                    //    }
                    //}
                }
            }
        }
    }
}

fn body(page_title: &str, content: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        div id="wrapper" {
            // Navigation
            nav class="navbar navbar-default navbar-static-top" role="navigation" style="margin-bottom: 0" {
                (navbar_header())
                (navbar_top())
                (navbar_side())
            }

            div id="page-wrapper" {
                div class="row" {
                    div class="col-md-12" {
                        h1 class="page-header" (page_title)
                    }
                }
                div class="row" {
                    div class="col-md-12" {
                        (content)
                    }
                }
                div class="row" {
                    div class="col-md-12" {}
                }
                div class="row" style="border-top: 1px solid #eee; margin-top: 0.2in"  {
                    div class="col-md-4" style="text-align: center;" { "Kafka-web" }
                    div class="col-md-4" style="text-align: center;" {
                        "Version: " (option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"))
                    }
                    div class="col-md-4" style="text-align: center;" {
                        "Request time: " span id="request_time" "loading"
                    }
                }
            }
        }

        script src="/public/sb-admin-2/vendor/jquery/jquery.min.js" {}
        script src="/public/sb-admin-2/vendor/bootstrap/js/bootstrap.min.js" {}
        script src="/public/sb-admin-2/vendor/metisMenu/metisMenu.min.js" {}
        script src="/public/sb-admin-2/vendor/datatables/js/jquery.dataTables.min.js" {}
        script src="/public/sb-admin-2/vendor/datatables-plugins/dataTables.bootstrap.min.js" {}
        script src="/public/sb-admin-2/vendor/datatables-responsive/dataTables.responsive.js" {}
        script src="/public/sb-admin-2/dist/js/sb-admin-2.js" {}
        script src="/public/my_js.js" {}
    }
}

pub fn page(req: &Request, page_title: &str, page_content: PreEscaped<String>) -> PreEscaped<String> {
    let request_timer = req.extensions.get::<RequestTimer>();
    let request_id = request_timer.map(|t| t.request_id).unwrap_or(-1);
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html {
            (html_head(page_title))
            body (body(page_title, page_content))
            span id="request_id" style="display: none" (request_id)
        }
    }
}
