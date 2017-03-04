#![feature(proc_macro)]

#[macro_use] extern crate log;
extern crate env_logger;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_cbor;
extern crate serde_json;
extern crate serde_transcode;

use clap::{App, Arg};
use futures::stream::Stream;
use rdkafka::consumer::{Consumer, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::util::get_rdkafka_version;
use std::io::Write;


#[derive(Deserialize)]
struct WrappedKey(String, Vec<u8>);

fn cbor_to_json_str(cbor: &[u8]) -> String {
    let mut deserializer = serde_cbor::de::Deserializer::new(cbor);
    let mut json_vec = Vec::new();
    {
        let mut serializer = serde_json::Serializer::new(&mut json_vec);
        if let Err(e) = serde_transcode::transcode(&mut deserializer, &mut serializer) {
            println!("Error: {:?}", e);
            return "ERR".to_string();
        };
        serializer.into_inner().flush().unwrap();
    }
    String::from_utf8(json_vec).unwrap()
}

fn parse_key(cbor: &[u8]) -> (String, String) {
    let wrapped_key = serde_cbor::from_slice::<WrappedKey>(cbor);
    if let Err(e) = wrapped_key {
        println!("Error parsing wrapped key: {:?}", e);
        return ("?".to_owned(), "?".to_owned());
    }
    let wrapped_key = wrapped_key.unwrap();

    (wrapped_key.0.clone(), cbor_to_json_str(&wrapped_key.1))
}

fn consume_and_print(brokers: &str, topics: &Vec<&str>, filter: Option<&str>) {
    let mut consumer = ClientConfig::new()
        .set("group.id", "topic_reader_group5")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("api.version.request", "true")
        .set_default_topic_config(TopicConfig::new()
            .set("auto.offset.reset", "smallest")
            .finalize())
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topics");

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
                let (cache_name, cache_key) = parse_key(key);
                if filter.is_some() && filter.unwrap() !=  cache_name {
                    // consumer.commit_message(&m, CommitMode::Async);
                    continue
                }
                println!("\n#### {}:{}, o:{}, s:{:.3}KB", topics[0], m.partition(), m.offset(),
                         (m.payload_len() as f64 / 1000f64));
                println!("{}: {}", cache_name, cache_key);
                let payload_dec = cbor_to_json_str(payload);
                if payload_dec.len() > 600 {
                    println!("{}...", &payload_dec[..600]);
                } else {
                    println!("{}", payload_dec);
                }
                // consumer.commit_message(&m, CommitMode::Async);
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
        .arg(Arg::with_name("topics")
             .short("t")
             .long("topics")
             .help("Topic list")
             .takes_value(true)
             .multiple(true)
             .required(true))
        .arg(Arg::with_name("filter")
            .short("f")
            .long("filter")
            .help("Filter only cache name")
            .takes_value(true))
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let filter = matches.value_of("filter");

    consume_and_print(brokers, &topics, filter);
}
