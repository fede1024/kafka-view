extern crate log;
extern crate env_logger;
extern crate chrono;

use self::chrono::Local;
use self::log::{LogRecord, LogLevelFilter};
use self::env_logger::LogBuilder;

use std::thread;

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>, date_format: &str) {
    let date_format = date_format.to_owned();
    let output_format = move |record: &LogRecord| {
        let thread_name = if log_thread {
            format!("({}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };
        let date = Local::now().format(&date_format).to_string();
        format!("{}: {}{} - {} - {}", date, thread_name, record.level(), record.target(), record.args())
    };

    let mut builder = LogBuilder::new();
    builder.format(output_format).filter(None, LogLevelFilter::Info);

    rust_log.map(|conf| builder.parse(conf));

    builder.init().unwrap();
}
