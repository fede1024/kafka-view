#[macro_use] extern crate log;
extern crate env_logger;
extern crate clap;
extern crate futures;
extern crate rdkafka;
extern crate serde;
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


fn cbor_to_json_str(cbor: &[u8]) -> String {
    let mut deserializer = serde_cbor::de::Deserializer::new(cbor);
    let mut json_vec = Vec::new();
    {
        let mut serializer = serde_json::Serializer::new(&mut json_vec);
        serde_transcode::transcode(&mut deserializer, &mut serializer).unwrap();
        serializer.into_inner().flush().unwrap();
    }
    String::from_utf8(json_vec).unwrap()
}

fn consume_and_print(brokers: &str, topics: &Vec<&str>) {
    let mut consumer = ClientConfig::new()
        .set("group.id", "topic_reader_group")
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_default_topic_config(TopicConfig::new()
            .set("auto.offset.reset", "smallest")
            .finalize())
        .create::<StreamConsumer<_>>()
        .expect("Consumer creation failed");

    consumer.subscribe(topics).expect("Can't subscribe to specified topics");

    // consumer.start() returns a stream. The stream can be used ot chain together expensive steps,
    // such as complext computations on a thread pool or asynchronous IO.
    let message_stream = consumer.start();

    for message in message_stream.wait() {
        match message {
            Err(e) => {
                warn!("Can't receive message: {:?}", e);
            },
            Ok(m) => {
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
                println!("\n#### partition: {}, offset: {}", m.partition(), m.offset());
                println!("Key wrapper: {}", cbor_to_json_str(key));
                println!("Payload: {}...", &cbor_to_json_str(payload)[..120]);
                consumer.commit_message(&m, CommitMode::Async);
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
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();

    consume_and_print(brokers, &topics);
}
