use maud::PreEscaped;

pub fn page(content: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html {
            head (header("Kafka-web"))
            body (body(content))
        }
    }
}

fn header(title: &str) -> PreEscaped<String> {
    html! {
        meta charset="utf-8" {}
        meta http-equiv="X-UA-Compatible" content="IE=edge" {}
        meta name="viewport" content="width=device-width, initial-scale=1" {}
        title (title)
        link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css"
            integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u"
            crossorigin="anonymous" {}
        link href="/public/css/dashboard.css" rel="stylesheet" {}
    }
}

fn body(content: PreEscaped<String>) -> PreEscaped<String> {
    html! {
        nav class="navbar navbar-inverse navbar-fixed-top" {
            div class="container-fluid" {
                div class="navbar-header" {
                    button type="button" class="navbar-toggle collapsed" data-toggle="collapse"
                            data-target="#navbar" aria-expanded="false" aria-controls="navbar" {
                        span class="sr-only" "Toggle navigation"
                        span class="icon-bar" ""  // Bars in menu button
                        span class="icon-bar" ""
                        span class="icon-bar" ""
                    }
                    a class="navbar-brand" href="#kafka_web" {
                        img src="/public/images/kafka_logo_small_white.png" style="float:left;max-width:170%;max-height:170%; margin-top: -0.06in" align="bottom"
                        "Kafka-web"
                    }
                }
                div id="navbar" class="navbar-collapse collapse" {
                    ul class="nav navbar-nav navbar-right" {
                        li a href="#dashboard" "Dashboard"
                        li a href="#settings" "Settings"
                        li a href="#profile" "Profile"
                        li a href="#help" "Help"
                    }
                    form class="navbar-form navbar-right" {
                        input type="text" class="form-control" placeholder="Search..." ""
                    }
                }
            }
        }

        div class="container-fluid" {
            div class="row" {
                div class="col-sm-3 col-md-2 sidebar" {
                    ul class="nav nav-sidebar" {
                        li class="active" { a href="#" { "Overview" span class="sr-only" "(current)" } }
                        li a href="#" "Reports"
                        li a href="#" "Analytics"
                        li a href="#" "Export"
                    }
                    ul class="nav nav-sidebar" {
                        li a href="" "Nav item"
                        li a href="" "Nav item again"
                        li a href="" "One more nav"
                        li a href="" "Another nav item"
                        li a href="" "More navigation"
                    }
                    ul class="nav nav-sidebar" {
                        li a href="" "Nav item again"
                        li a href="" "One more nav"
                        li a href="" "Another nav item"
                    }
                }
                div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main" {
                    (content)
                }
            }
        }

        script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js" {}
        script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"
            integrity="sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa"
            crossorigin="anonymous" {}
    }
}
