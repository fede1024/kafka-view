#[macro_use] extern crate log;
extern crate env_logger;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate byteorder;

use clap::{App, Arg};
use futures::stream::Stream;
use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::util::get_rdkafka_version;
use std::str;

use std::io::{self, Cursor, Seek, SeekFrom};
use byteorder::{BigEndian, ReadBytesExt};


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


fn read_string<'a>(rdr: &'a mut Cursor<&[u8]>) -> Result<&'a str, ParserError> {
    let strlen = try!(rdr.read_i16::<BigEndian>()) as usize;
    let pos = rdr.position() as usize;
    let slice = try!(str::from_utf8(&rdr.get_ref()[pos..(pos+strlen)]));
    rdr.seek(SeekFrom::Current(strlen as i64));
    Ok(slice)
}

fn parse_group_offset(key_rdr: &mut Cursor<&[u8]>, payload: &[u8]) -> Result<(), ParserError> {
    let group = try!(read_string(key_rdr)).to_owned();
    let topic = try!(read_string(key_rdr)).to_owned();
    let partition = try!(key_rdr.read_i32::<BigEndian>());
    println!("group offset {:?} {:?} {}", group, topic, partition);
    Ok(())
}

fn parse_group_metadata(key_rdr: &mut Cursor<&[u8]>, payload: &[u8]) -> Result<(), ParserError> {
    let group = read_string(key_rdr);
    println!("group metadata {:?}", group);
    Ok(())
}

fn parse_message(key: &[u8], payload: &[u8]) -> Result<(), ParserError> {
    let mut key_rdr = Cursor::new(key);
    let key_version = try!(key_rdr.read_i16::<BigEndian>());
    println!(">> {:?}", key_version);
    match key_version {
        0 | 1 => Ok(try!(parse_group_offset(&mut key_rdr, payload))),
        2 => Ok(try!(parse_group_metadata(&mut key_rdr, payload))),
        _ => Err(ParserError::Format),
    }
}

fn consume_and_print(brokers: &str) {
    let mut consumer = ClientConfig::new()
        .set("group.id", "consumer_reader_group")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_default_topic_config(TopicConfig::new()
            .set("auto.offset.reset", "smallest")
            .finalize())
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");

    consumer.subscribe(&vec!["__consumer_offsets"])
        .expect("Can't subscribe to specified topics");

    for message in consumer.start().wait() {
        match message {
            Err(e) => {
                warn!("Can't receive data from stream: {:?}", e);
            },
            Ok(Ok(m)) => {
                let key = match m.key_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message key: {:?}", e);
                        &[]
                    },
                };
                let payload = match m.payload_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("Error while deserializing message payload: {:?}", e);
                        &[]
                    },
                };
                println!("\n#### P:{}, o:{}, s:{:.3}KB", m.partition(), m.offset(),
                         (m.payload_len() as f64 / 1000f64));

                parse_message(key, payload);

            },
            Ok(Err(e)) => {
                warn!("Kafka error: {:?}", e);
            },
        };
    }
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(Arg::with_name("brokers")
             .short("b")
             .long("brokers")
             .help("Broker list in kafka format")
             .takes_value(true)
             .default_value("localhost:9092"))
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let brokers = matches.value_of("brokers").unwrap();

    consume_and_print(brokers);
}

