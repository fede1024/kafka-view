use brotli;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::Local;
use env_logger::Builder;
use log::{LevelFilter, Record};
use rocket::http::{ContentType, Status};
use rocket::response::{self, Responder};
use rocket::{fairing, Data, Request, Response};

use std::env;
use std::io::{self, BufRead, Cursor, Read, Write};
use std::str;
use std::thread;

use env_logger::fmt::Formatter;
use crate::error::*;

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>, date_format: &str) {
    let date_format = date_format.to_owned();
    let output_format = move |buffer: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("({}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };
        let date = Local::now().format(&date_format).to_string();
        writeln!(
            buffer,
            "{}: {}{} - {} - {}",
            date,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);
    if env::var("ROCKET_ENV")
        .map(|var| !var.starts_with("dev"))
        .unwrap_or(false)
    {
        // _ is used in Rocket as a special target for debugging purpose
        builder.filter(Some("_"), LevelFilter::Error);
    }

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
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
    }};
}

macro_rules! time {
    ($title:expr, $msg:expr) => {{
        use chrono;
        let start_time = chrono::Utc::now();
        let ret = $msg;
        let elapsed_micros = chrono::Utc::now()
            .signed_duration_since(start_time)
            .num_microseconds()
            .unwrap() as f32;
        debug!(
            "Elapsed time while {}: {:.3}ms",
            $title,
            elapsed_micros / 1000f32
        );
        ret
    }};
}

/// Given a vector, will insert `value` at the desired position `pos`, filling the items
/// with `default`s if needed.
pub fn insert_at<T: Copy>(vector: &mut Vec<T>, pos: usize, value: T, default: T) {
    for _ in vector.len()..=pos {
        vector.push(default);
    }
    vector[pos] = value;
}


pub fn read_str<'a>(rdr: &'a mut Cursor<&[u8]>) -> Result<&'a str> {
    let len = (rdr.read_i16::<BigEndian>()).chain_err(|| "Failed to parse string len")? as usize;
    let pos = rdr.position() as usize;
    let slice = str::from_utf8(&rdr.get_ref()[pos..(pos + len)])
        .chain_err(|| "String is not valid UTF-8")?;
    rdr.consume(len);
    Ok(slice)
}

pub fn read_string(rdr: &mut Cursor<&[u8]>) -> Result<String> {
    read_str(rdr).map(str::to_string)
}

// GZip compression fairing
pub struct GZip;
#[rocket::async_trait]
impl fairing::Fairing for GZip {
    fn info(&self) -> fairing::Info {
        fairing::Info {
            name: "GZip compression",
            kind: fairing::Kind::Response,
        }
    }

    async fn on_response<'r>(&self, request: &'r Request<'_>, response: &mut Response<'r>) {
        use flate2::{Compression,FlateReadExt};
        use std::io::{Cursor, Read};
        let headers = request.headers();
        if headers
            .get("Accept-Encoding")
            .any(|e| e.to_lowercase().contains("gzip"))
        {
            response.body_mut().to_bytes().await.and_then(|body| {
                let mut enc = body.gz_encode(Compression::Default);
                let mut buf = Vec::with_capacity(body.len());
                enc.read_to_end(&mut buf)
                    .map(|_| {
                        response.set_sized_body(buf.len(),Cursor::new(buf));
                        response.set_raw_header("Content-Encoding", "gzip");
                    })
                    .map_err(|e| eprintln!("{}", e))
                    .ok();

                Ok(())
            });
        }
    }
    }

// Request logging
pub struct RequestLogger;
#[rocket::async_trait]
impl fairing::Fairing for RequestLogger {
    // This is a request and response fairing named "GET/POST Counter".
    fn info(&self) -> fairing::Info {
        fairing::Info {
            name: "User request logger",
            kind: fairing::Kind::Request,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, _: &mut Data<'_>) {
        let uri = request.uri().path();
        if !uri.starts_with("/api") && !uri.starts_with("/public") {
            info!("User request: {}", uri);
        }
    }
}
