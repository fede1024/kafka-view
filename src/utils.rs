use chrono::Local;
use env_logger::LogBuilder;
use log::{LogRecord, LogLevelFilter};

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

macro_rules! format_error_chain {
    ($err: expr) => {{
        error ! ("error: {}", $err);
        for e in $err.iter().skip(1) {
            error ! ("caused by: {}", e);
        }
        if let Some(backtrace) = $err.backtrace() {
            error ! ("backtrace: {:?}", backtrace);
        }
    }}
}

macro_rules! time {
    ($title:expr, $msg:expr) => {{
        use chrono;
        let start_time = chrono::UTC::now();
        let ret = $msg;
        let elapsed_micros = chrono::UTC::now().signed_duration_since(start_time).num_microseconds().unwrap() as f32;
        debug!("Elapsed time while {}: {:.3}ms", $title, elapsed_micros / 1000f32);
        ret
    }};
}

/// Given a vector, will insert `value` at the desired position `pos`, filling the items
/// with `default`s if needed.
pub fn insert_at<T: Copy>(vector: &mut Vec<T>, pos: usize, value: T, default: T) {
    for _ in vector.len()..(pos+1) {
        vector.push(default);
    }
    vector[pos] = value;
}
