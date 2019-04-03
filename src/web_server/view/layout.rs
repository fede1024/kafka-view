use crate::RUST_VERSION;
use maud::{self, html, PreEscaped};

pub fn search_form(
    action: &str,
    placeholder: &str,
    value: &str,
    regex: bool,
) -> PreEscaped<String> {
    html! {
        form action=(action) {
            div class="row" {
                div class="col-md-12" style="margin-top: 20pt" {}
            }
            div class="row" {
                div class="col-md-2" { "" }
                div class="col-md-8" {
                    div class="input-group custom-search-form" {
                        input class="form-control" type="text" name="string" style="font-size: 18pt; height: 30pt"
                            placeholder=(placeholder) value=(value) {
                            span class="input-group-btn" {
                                button class="btn btn-default" style="height: 30pt" type="submit" {
                                    i class="fa fa-search fa-2x" {}
                                }
                            }
                        }
                    }
                }
                div class="col-md-2" {}
            }
            div class="row" {
                div class="col-md-2" { "" }
                div class="col-md-8" style="margin-top: 10pt" {
                    strong { "Search options:" }
                    label class="checkbox-inline" style="margin-left: 10pt" {
                        @if regex {
                            input type="checkbox" name="regex" checked="" {}
                        } @else {
                            input type="checkbox" name="regex" {}
                        }
                        "Regex"
                    }
                }
                div class="col-md-2" { "" }
            }
            div class="row" {
                div class="col-md-12" style="margin-top: 20pt" {}
            }
        }
    }
}

pub fn notification(n_type: &str, content: PreEscaped<String>) -> PreEscaped<String> {
    let alert_class = format!("alert alert-{}", n_type);
    html! {
        div class=(alert_class) {
            (content)
        }
    }
}

pub fn datatable_ajax(
    id: &str,
    url: &str,
    param: &str,
    table_header: PreEscaped<String>,
) -> PreEscaped<String> {
    let table_id = format!("datatable-{}", id);
    html! {
        table id=(table_id) data-url=(url) data-param=(param) width="100%" class="table table-striped table-bordered table-hover" {
            thead { (table_header) }
        }
    }
}

pub fn panel(heading: PreEscaped<String>, body: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        div class="panel panel-default" {
            div class="panel-heading" {
                (heading)
                div class="pull-right" {
                    div class="btn-group" {
                        button id="tailer_button_label" type="button"
                            class="btn btn-default btn-xs dropdown-toggle" data-toggle="dropdown" {
                            "Topic tailer: active" span class="caret" {}
                        }
                        ul class="dropdown-menu pull-right" role="menu" {
                            li id="start_tailer_button" { a href="#" { "Start" } }
                            li id="stop_tailer_button" { a href="#" { "Stop" } }
                            // li a href="#" "Action"
                            // li a href="#" "Action"
                            // li class="divider" {}
                            // li a href="#" "Action"
                        }
                    }
                }
            }
            div class="panel-body" { (body) }
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
            title { (title) }
            link href="/public/sb-admin-2/vendor/bootstrap/css/bootstrap.min.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/vendor/metisMenu/metisMenu.min.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/vendor/datatables-plugins/dataTables.bootstrap.css" rel="stylesheet" {}
            // link href="/public/sb-admin-2/vendor/datatables/css/jquery.dataTables.min.css" rel="stylesheet" {}
            // link href="/public/sb-admin-2/vendor/datatables/css/dataTables.jqueryui.min.css" rel="stylesheet" {}
            link href="/public/sb-admin-2/dist/css/sb-admin-2.css" rel="stylesheet" {}
            link href="/public/css/font-awesome.min.css" rel="stylesheet" type="text/css" {}
            link href="/public/my_css.css" rel="stylesheet" type="text/css" {}
            script async="" defer="" src="https://buttons.github.io/buttons.js" {}
        }
    }
}

fn navbar_header() -> PreEscaped<String> {
    html! {
        div class="navbar-header" {
            button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse" {
                span class="sr-only" { "Toggle navigation" }
                span class="icon-bar" {}
                span class="icon-bar" {}
                span class="icon-bar" {}
            }
            a class="navbar-brand" href="/" {
                    img src="/public/images/kafka_logo.png"
                        style="float:left;max-width:160%;max-height:160%; margin-top: -0.06in; margin-right: 0.07in"
                        align="bottom"
                { "Kafka-view" }
            }
        }
    }
}

fn navbar_top() -> PreEscaped<String> {
    html! {
        ul class="nav navbar-top-links navbar-right" {
            li class="dropdown" {
                a class="dropdown-toggle" style="font-size: 12pt" data-toggle="dropdown" href="#" {
                    i class="fa fa-question-circle-o fa-fw" {}
                    i class="fa fa-caret-down" {}
                }
                ul class="dropdown-menu dropdown-user" {
                    li { a href="https://github.com/fede1024/kafka-view" {
                        i class="fa fa-github fa-fw" {} "GitHub" }
                    }
                    // li class="divider" {}
                    // li { a href="#" {i class="fa fa-sign-out fa-fw" {} "Logout" } }
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
                        form action="/omnisearch" {
                            div class="input-group custom-search-form" {
                                input type="text" name="string" class="form-control" placeholder="Omnisearch..." {
                                    span class="input-group-btn" {
                                        button class="btn btn-default" type="submit" {
                                            i class="fa fa-search" {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // li a href="/" { i class="fa fa-dashboard fa-fw" {}  " Home" }
                    //li a href="/" style="font-size: 12pt" { i class="fa fa-info-circle fa-fw" {}  " Home" }
                    li { a href="/clusters/" style="font-size: 12pt" { i class="fa fa-server fa-fw" {}  " Clusters" } }
                    li { a href="/topics/" style="font-size: 12pt" { i class="fa fa-cubes fa-fw" {}  " Topics" } }
                    li { a href="/consumers/" style="font-size: 12pt" { i class="fa fa-exchange fa-fw" {}  " Consumers" } }
                    li {
                        a href="#" style="font-size: 12pt" {
                            i class="fa fa-gear fa-fw" {} " Internals"
                            span class="fa arrow" {}
                        }
                        ul class="nav nav-second-level" {
                            li {
                                a href="/internals/caches" {
                                    i class="fa fa-microchip fa-fw" { {}  " Caches" }
                                }
                            }
                            li {
                                a href="/internals/live_consumers" {
                                    i class="fa fa-microchip fa-fw" {}  " Live consumers"
                                }
                            }
                            // li {
                            //     a href="#" { "Third Level" span class="fa arrow" {} }
                            //     ul class="nav nav-third-level" {
                            //         li a href="#" "Third Level Item"
                            //         li a href="#" "Third Level Item"
                            //         li a href="#" "Third Level Item"
                            //         li a href="#" "Third Level Item"
                            //     }
                            // }
                        }
                    }
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

            div id="page-wrapper" class="flex-container" {
                div class="row" {
                    div class="col-md-12" {
                        h1 class="page-header" { (page_title) }
                    }
                }
                div class="row flex-body" {
                    div class="col-md-12" {
                        (content)
                    }
                }
                div class="row" {
                    div class="col-md-12" {}
                }
                div class="row flex-footer" style="border-top: 1px solid #eee; margin-top: 0.2in; padding-top: 0.05in"  {
                    div class="col-md-4" style="text-align: center;" {
                        a href="https://github.com/fede1024/kafka-view" {
                            "kafka-view " (option_env!("CARGO_PKG_VERSION").unwrap_or("")) }
                        }
                    div class="col-md-4" style="text-align: center;" {
                        a href="https://www.rust-lang.org" { (RUST_VERSION) }
                    }
                    div class="col-md-4" style="text-align: center;" {
                        a class="github-button" href="https://github.com/fede1024/kafka-view"
                            data-icon="octicon-star" data-count-href="/fede1024/kafka-view/stargazers"
                            data-show-count="true"
                            data-count-aria-label="# stargazers on GitHub" // data-style="mega"
                            aria-label="Star fede1024/kafka-view on GitHub" { "Star" }
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
        // (PreEscaped("<script async defer src=\"https://buttons.github.io/buttons.js\">"))
        script src="/public/my_js.js" {}
    }
}

pub fn page(page_title: &str, page_content: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        (maud::DOCTYPE)
        html {
            (html_head(page_title))
            body { (body(page_title, page_content)) }
        }
    }
}
