use chrono::Local;
use env_logger::LogBuilder;
use log::{LogRecord, LogLevelFilter};

use rocket::response::{self, Responder};
use rocket::{Request, Response};
use rocket::http::{ContentType, Status};

use flate2::Compression;
use flate2::write::ZlibEncoder;

use std::thread;
use std::io::Write;

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


pub struct GZippedString(pub String);

/// Serializes the wrapped value into gzip compressed JSON. Returns a response with Content-Type
/// JSON and a fixed-size body with the serialized value. If serialization
/// fails, an `Err` of `Status::InternalServerError` is returned.
impl Responder<'static> for GZippedString {
    fn respond_to(self, req: &Request) -> response::Result<'static> {
        let headers = req.headers();
        // check if requests accepts gzip encoding
        println!(">> headers {:?}", headers);
        println!(">> contains {:?}", headers.contains("Accept"));
        println!(">> get {:?}", headers.get("Accept-Encoding").collect::<Vec<_>>());
        println!(">> any {:?}", headers.get("Accept-Encoding")
            .map(|e| e.to_uppercase())
            .collect::<Vec<_>>());
        if headers.contains("Accept") &&
            headers.get("Accept-Encoding").any(|e| e.contains("gzip")) {
            println!(">> HERE");
            // let data = ::deflate::deflate_bytes_gzip(self.0.as_bytes());
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::Default);
            encoder.write(self.0.as_bytes());
            let data = encoder.finish()  // Maybe just log and revert to non-gzip
                .map_err(|e| {
                    error!("GZip compression failed {:?}", e);
                    Status::InternalServerError
                })?;
            println!(">> {:?}", data);
            Ok(Response::build()
                .status(Status::Ok)
                .header(ContentType::JSON)
                .raw_header("Content-Encoding", "gzip")
                .sized_body(::std::io::Cursor::new(data))
                .finalize())
        } else {
            Ok(Response::build()
                .status(Status::Ok)
                .header(ContentType::JSON)
                .sized_body(::std::io::Cursor::new(self.0))
                .finalize())
        }
    }
}

