use brotli;
use chrono::Local;
use env_logger::LogBuilder;
use log::{LogRecord, LogLevelFilter};
use rocket::http::{ContentType, Status};
use rocket::response::{self, Responder};
use rocket::{Request, Response};
use serde_json;

use std::thread;
use std::io;

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
        error!("error: {}", $err);
        for e in $err.iter().skip(1) {
            error!("caused by: {}", e);
        }
        if let Some(backtrace) = $err.backtrace() {
            error!("backtrace: {:?}", backtrace);
        }
    }}
}

macro_rules! time {
    ($title:expr, $msg:expr) => {{
        use chrono;
        let start_time = chrono::Utc::now();
        let ret = $msg;
        let elapsed_micros = chrono::Utc::now().signed_duration_since(start_time).num_microseconds().unwrap() as f32;
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

/// Wraps a JSON value and implements a responder for it, with support for brotli compression.
pub struct CompressedJSON(pub serde_json::Value);

impl Responder<'static> for CompressedJSON {
    fn respond_to(self, req: &Request) -> response::Result<'static> {
        let json = serde_json::to_vec(&self.0).unwrap();
        let reader = io::Cursor::new(json);
        let headers = req.headers();
        if headers.contains("Accept") && headers.get("Accept-Encoding").any(|e| e.contains("br")) {
            Ok(Response::build()
                .status(Status::Ok)
                .header(ContentType::JSON)
                .raw_header("Content-Encoding", "br")
                .streamed_body(brotli::CompressorReader::new(reader, 4096, 3, 20))
                .finalize())
        } else {
            Ok(Response::build()
                .status(Status::Ok)
                .header(ContentType::JSON)
                .streamed_body(reader)
                .finalize())
        }
    }
}
