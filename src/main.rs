#![feature(alloc_system, proc_macro)]
extern crate alloc_system;

#[macro_use] extern crate log;
extern crate env_logger;
extern crate rdkafka;

#[macro_use] extern crate serde_derive;
extern crate serde_json;

mod scheduler;
mod config;

use scheduler::ScheduledTask;
use scheduler::Scheduler;

use std::time;
use std::thread;
use std::collections::HashMap;

use rdkafka::consumer::{BaseConsumer, EmptyConsumerContext};
use rdkafka::config::ClientConfig;
use rdkafka::error;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::*;

#[derive(Serialize, Deserialize, Debug)]
struct Partition {
    id: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    error: Option<String>
}

impl Partition {
    fn new(id: i32, leader: i32, replicas: Vec<i32>, isr: Vec<i32>, error: Option<String>) -> Partition {
        Partition {
            id: id,
            leader: leader,
            replicas: replicas,
            isr: isr,
            error: error
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Broker {
    id: i32,
    host: String,
    port: i32
}

impl Broker {
    fn new(id: i32, host: String, port: i32) -> Broker {
        Broker {
            id: id,
            host: host,
            port: port
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
    brokers: Vec<Broker>,
    topics: HashMap<String, Vec<Partition>>,
}

impl Metadata {
    fn new(brokers: Vec<Broker>, topics: HashMap<String, Vec<Partition>>) -> Metadata {
        Metadata {
            brokers: brokers,
            topics: topics,
        }
    }
}

fn fetch_metadata(consumer: &BaseConsumer<EmptyConsumerContext>) -> KafkaResult<Metadata> {
    let metadata = try!(consumer.fetch_metadata(10000));

    let mut brokers = Vec::new();
    for broker in metadata.brokers() {
        brokers.push(Broker::new(broker.id(), broker.host().to_owned(), broker.port()));
    }

    let mut topics = HashMap::new();
    for t in metadata.topics() {
        let mut topic = Vec::with_capacity(t.partitions().len());
        for p in t.partitions() {
            topic.push(Partition::new(p.id(), p.leader(), p.replicas().to_owned(), p.isr().to_owned(),
                                      p.error().map(|e| error::resp_err_description(e))));
        }
        topics.insert(t.name().to_owned(), topic);
    }

    Ok(Metadata::new(brokers, topics))
}


struct MetadataFetcher {
    brokers: String,
    consumer: Option<BaseConsumer<EmptyConsumerContext>>,
}

impl MetadataFetcher {
    fn new(brokers: &str) -> MetadataFetcher {
        MetadataFetcher {
            brokers: brokers.to_owned(),
            consumer: None,
        }
    }

    fn create_consumer(&mut self) {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create::<BaseConsumer<_>>()
            .expect("Consumer creation failed");
        self.consumer = Some(consumer);
    }
}

impl ScheduledTask for MetadataFetcher {
    fn run(&self) {
        info!("HI");
        let metadata = match self.consumer {
            None => {
                error!("Consumer not initialized");
                return;
            },
            Some(ref consumer) => fetch_metadata(consumer),
        };
        match metadata {
            Err(e) => error!("Error while fetching metadata {}", e),
            Ok(m) => {
                let serialized = serde_json::to_string(&m).unwrap();
                println!("serialized = {}", serialized);
            }
        }
    }
}

fn main() {
    env_logger::init().unwrap();

    let cluster_1 = "localhost:9092";
    let mut fetcher = MetadataFetcher::new(cluster_1);
    fetcher.create_consumer();

    //let mut sched = Scheduler::new(time::Duration::from_secs(3));
    // sched.add_task(cluster_1, fetcher);

    //thread::sleep_ms(10000);

    let p = config::read_config("clusters.yaml");

    println!(">> {:?}", p);
}
