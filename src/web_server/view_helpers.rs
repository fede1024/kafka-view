extern crate handlebars as hbs;

use self::hbs::{Handlebars, RenderError, RenderContext, Helper, Context, Renderable};

const FACTOR_OF_INTEREST_IDX: usize = 0;
const CANDIDATE_IDX: usize = 1;

pub fn each_sorted_helper(ctx: &Context, helper: &Helper, hbars: &Handlebars, render_ctx: &mut RenderContext) -> Result<(), RenderError> {
    let internal_block = helper.template();

    try!(render_ctx.writer.write("hola".as_bytes()));

    match internal_block {
        Some(t) => t.render(ctx, hbars, render_ctx),
        None => Ok(()),
    }
}

// #[cfg(test)]
// mod if_multiple_of_helper_tests {
//     extern crate handlebars as hbs;
// 
//     use hbs::{Handlebars, Template};
// 
//     #[derive(ToJson)]
//     struct TestCandidate {
//         value: i64
//     }
// 
//     #[test]
//     fn should_return_an_err_when_one_of_the_parameters_is_missing() {
//         let template = Template::compile(
//             "{{#if-multiple-of 2}}\
//                 IS_MULTIPLE\
//             {{else}}\
//                 IS_NOT_MULTIPLE\
//             {{/if-multiple-of}}".to_string());
// 
//         let mut hbs = Handlebars::new();
//         hbs.register_template("template", template.unwrap());
//         hbs.register_helper("if-multiple-of", Box::new(super::if_multiple_of_helper));
// 
//         let rendered = hbs.render("template", &TestCandidate { value: 2});
// 
//         assert!(rendered.is_err());
//     }
// 
//     #[test]
//     fn should_return_an_err_when_the_candidate_is_less_than_0() {
//         let template = Template::compile(
//             "{{#if-multiple-of 2 this.value}}\
//                 IS_MULTIPLE\
//             {{else}}\
//                 IS_NOT_MULTIPLE\
//             {{/if-multiple-of}}".to_string());
// 
//         let mut hbs = Handlebars::new();
//         hbs.register_template("template", template.unwrap());
//         hbs.register_helper("if-multiple-of", Box::new(super::if_multiple_of_helper));
// 
//         let rendered = hbs.render("template", &TestCandidate { value: -3});
// 
//         assert!(rendered.is_err());
//     }
// 
//     #[test]
//     fn should_return_an_err_when_the_factor_of_interest_is_0() {
//         let template = Template::compile(
//             "{{#if-multiple-of 0 this.value}}\
//                 IS_MULTIPLE\
//             {{else}}\
//                 IS_NOT_MULTIPLE\
//             {{/if-multiple-of}}".to_string());
// 
//         let mut hbs = Handlebars::new();
//         hbs.register_template("template", template.unwrap());
//         hbs.register_helper("if-multiple-of", Box::new(super::if_multiple_of_helper));
// 
//         let rendered = hbs.render("template", &TestCandidate { value: 3});
// 
//         assert!(rendered.is_err());
//     }
// 
//     #[test]
//     fn should_return_an_err_when_the_factor_of_interest_is_less_than_0() {
//         let template = Template::compile(
//             "{{#if-multiple-of -1 this.value}}\
//                 IS_MULTIPLE\
//             {{else}}\
//                 IS_NOT_MULTIPLE\
//             {{/if-multiple-of}}".to_string());
// 
//         let mut hbs = Handlebars::new();
//         hbs.register_template("template", template.unwrap());
//         hbs.register_helper("if-multiple-of", Box::new(super::if_multiple_of_helper));
// 
//         let rendered = hbs.render("template", &TestCandidate { value: 3});
// 
//         assert!(rendered.is_err());
//     }
// 
//     #[test]
//     fn should_return_the_is_not_multiple_template_when_the_candidate_is_a_multiple_of_the_factor() {
//         let template = Template::compile(
//             "{{#if-multiple-of 2 this.value}}\
//                 IS_MULTIPLE\
//             {{else}}\
//                 IS_NOT_MULTIPLE\
//             {{/if-multiple-of}}".to_string());
// 
//         let mut hbs = Handlebars::new();
//         hbs.register_template("template", template.unwrap());
//         hbs.register_helper("if-multiple-of", Box::new(super::if_multiple_of_helper));
// 
//         let rendered = hbs.render("template", &TestCandidate { value: 3});
// 
//         assert_eq!("IS_NOT_MULTIPLE", rendered.ok().unwrap());
//     }
// 
//     #[test]
//     fn should_return_the_is_multiple_template_when_the_candidate_is_a_multiple_of_the_factor() {
//         let template = Template::compile(
//             "{{#if-multiple-of 2 this.value}}\
//                 IS_MULTIPLE\
//             {{else}}\
//                 IS_NOT_MULTIPLE\
//             {{/if-multiple-of}}".to_string());
// 
//         let mut hbs = Handlebars::new();
//         hbs.register_template("template", template.unwrap());
//         hbs.register_helper("if-multiple-of", Box::new(super::if_multiple_of_helper));
// 
//         let rendered = hbs.render("template", &TestCandidate { value: 2});
// 
//         assert_eq!("IS_MULTIPLE", rendered.ok().unwrap());
//     }
// }
