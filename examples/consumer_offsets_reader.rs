#[macro_use]
extern crate log;
extern crate byteorder;
extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate rdkafka;

use byteorder::{BigEndian, ReadBytesExt};
use clap::{App, Arg};
use futures::stream::Stream;
use rdkafka::config::{ClientConfig};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::util::get_rdkafka_version;
use rocket::futures::StreamExt;

use std::io::{self, BufRead, Cursor};
use std::str;

#[derive(Debug)]
enum ParserError {
    Format,
    Io(io::Error),
    Utf8(str::Utf8Error),
}

impl From<io::Error> for ParserError {
    fn from(err: io::Error) -> ParserError {
        ParserError::Io(err)
    }
}

impl From<str::Utf8Error> for ParserError {
    fn from(err: str::Utf8Error) -> ParserError {
        ParserError::Utf8(err)
    }
}

#[derive(Debug)]
enum ConsumerUpdate {
    Metadata,
    SetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    DeleteCommit {
        group: String,
        topic: String,
        partition: i32,
    },
}

fn read_str<'a>(rdr: &'a mut Cursor<&[u8]>) -> Result<&'a str, ParserError> {
    let strlen = rdr.read_i16::<BigEndian>()? as usize;
    let pos = rdr.position() as usize;
    let slice = str::from_utf8(&rdr.get_ref()[pos..(pos + strlen)])?;
    rdr.consume(strlen);
    Ok(slice)
}

fn parse_group_offset(
    key_rdr: &mut Cursor<&[u8]>,
    payload_rdr: &mut Cursor<&[u8]>,
) -> Result<ConsumerUpdate, ParserError> {
    let group = read_str(key_rdr)?.to_owned();
    let topic = read_str(key_rdr)?.to_owned();
    let partition = key_rdr.read_i32::<BigEndian>()?;
    if !payload_rdr.get_ref().is_empty() {
        payload_rdr.read_i16::<BigEndian>()?;
        let offset = payload_rdr.read_i64::<BigEndian>()?;
        Ok(ConsumerUpdate::SetCommit {
            group: group,
            topic: topic,
            partition: partition,
            offset: offset,
        })
    } else {
        Ok(ConsumerUpdate::DeleteCommit {
            group: group,
            topic: topic,
            partition: partition,
        })
    }
}

fn parse_message(key: &[u8], payload: &[u8]) -> Result<ConsumerUpdate, ParserError> {
    let mut key_rdr = Cursor::new(key);
    let mut payload_rdr = Cursor::new(payload);
    let key_version = key_rdr.read_i16::<BigEndian>()?;
    match key_version {
        0 | 1 => Ok(parse_group_offset(&mut key_rdr, &mut payload_rdr)?),
        2 => Ok(ConsumerUpdate::Metadata),
        _ => Err(ParserError::Format),
    }
}

async fn consume_and_print(brokers: &str) {
    let consumer = ClientConfig::new()
        .set("group.id", "consumer_reader_group")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "smallest")
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec!["__consumer_offsets"])
        .expect("Can't subscribe to specified topics");
    let mut stream = consumer.stream();
    loop {
        let message = stream.next().await;
        match message {
            None =>{break;}
            Some(Err(e)) => {
                warn!("Can't receive data from stream: {:?}", e);
                break;
            }
            Some(Ok(m)) => {
                let key = match m.key_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message key: {:?}", e);
                        &[]
                    }
                };
                let payload = match m.payload_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        &[]
                    }
                };
                println!(
                    "\n#### P:{}, o:{}, s:{:.3}KB",
                    m.partition(),
                    m.offset(),
                    (m.payload_len() as f64 / 1000f64)
                );

                let msg = parse_message(key, payload);
                println!("{:?}", msg);
            }
        };
    }
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let brokers = matches.value_of("brokers").unwrap();

    consume_and_print(brokers);
}
