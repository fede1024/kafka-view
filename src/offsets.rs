use futures::stream::Stream;
use rdkafka::consumer::{Consumer, CommitMode, EmptyConsumerContext};
use rdkafka::consumer::stream_consumer::{StreamConsumer, MessageStream};
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::util::get_rdkafka_version;
use std::cmp;
use std::str;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Instant, Duration};

use std::io::{Cursor, BufRead};
use byteorder::{BigEndian, ReadBytesExt};
use error::*;
use utils::format_error_chain;
use cache::OffsetsCache;
use metadata::{ClusterId, TopicName};


#[derive(Debug)]
enum ConsumerUpdate {
    Metadata,
    SetCommit { group: String, topic: String, partition: i32, offset: i64 },
    DeleteCommit { group: String, topic: String, partition: i32 },
}

fn read_str<'a>(rdr: &'a mut Cursor<&[u8]>) -> Result<&'a str> {
    let strlen = (rdr.read_i16::<BigEndian>()).chain_err(|| "Failed to parse string len")? as usize;
    let pos = rdr.position() as usize;
    let slice = str::from_utf8(&rdr.get_ref()[pos..(pos+strlen)])
        .chain_err(|| "String is not valid UTF-8")?;
    rdr.consume(strlen);
    Ok(slice)
}

fn parse_group_offset(key_rdr: &mut Cursor<&[u8]>,
                      payload_rdr: &mut Cursor<&[u8]>) -> Result<ConsumerUpdate> {
    let group = read_str(key_rdr).chain_err(|| "Failed to parse group name from key")?.to_owned();
    let topic = read_str(key_rdr).chain_err(|| "Failed to parse topic name from key")?.to_owned();
    let partition = key_rdr.read_i32::<BigEndian>().chain_err(|| "Failed to parse partition from key")?;
    if payload_rdr.get_ref().len() != 0 {
        let _version = payload_rdr.read_i16::<BigEndian>().chain_err(|| "Failed to parse value version")?;
        let offset = payload_rdr.read_i64::<BigEndian>().chain_err(|| "Failed to parse offset from value")?;
        Ok(ConsumerUpdate::SetCommit { group: group, topic: topic, partition: partition, offset: offset })
    } else {
        Ok(ConsumerUpdate::DeleteCommit { group: group, topic: topic, partition: partition })
    }
}

fn parse_message(key: &[u8], payload: &[u8]) -> Result<ConsumerUpdate> {
    let mut key_rdr = Cursor::new(key);
    let mut payload_rdr = Cursor::new(payload);
    let key_version = key_rdr.read_i16::<BigEndian>().chain_err(|| "Failed to parse key version")?;
    match key_version {
        0 | 1 => parse_group_offset(&mut key_rdr, &mut payload_rdr)
            .chain_err(|| "Failed to parse group offset update"),
        2 => Ok(ConsumerUpdate::Metadata),
        _ => bail!("Key version not recognized"),
    }
}

fn create_consumer(brokers: String) -> StreamConsumer<EmptyConsumerContext> {
    let mut consumer = ClientConfig::new()
        .set("group.id", "consumer_reader_group")
        .set("bootstrap.servers", &brokers)
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

    consumer
}

fn insert_at(v: &mut Vec<i64>, pos: usize, value: i64) {
    for _ in v.len()..(pos+1) {
        v.push(-1);
    }
    v[pos] = value;
}

// we should really have some tests here
fn update_global_cache(cluster_id: &ClusterId, local_cache: &HashMap<(ClusterId, String), Vec<i64>>,
                       cache: &OffsetsCache) {
    for (&(ref group, ref topic), offsets) in local_cache {   // Consider a consuming iterator
        // This logic is not needed if i store the consumer offset, right?
        if offsets.iter().any(|&offset| offset == -1) {
            if let Some(mut existing_offsets) = cache.get(&(cluster_id.to_owned(), group.to_owned(), topic.to_owned())) {
                // If the new offset is not complete and i have an old one, do the merge
                for i in 0..(cmp::max(offsets.len(), existing_offsets.len())) {
                    let new_offset = cmp::max(
                        offsets.get(i).cloned().unwrap_or(-1),
                        existing_offsets.get(i).cloned().unwrap_or(-1));
                    insert_at(&mut existing_offsets, i, new_offset);
                }
                cache.insert((cluster_id.to_owned(), group.to_owned(), topic.to_owned()), existing_offsets);
                continue;
            }
        }
        cache.insert((cluster_id.to_owned(), group.to_owned(), topic.to_owned()), offsets.clone());
    }
}

fn consume_offset_topic(cluster_id: ClusterId, mut consumer: StreamConsumer<EmptyConsumerContext>,
                        cache: OffsetsCache) -> Result<()> {
    let mut local_cache = HashMap::new();
    let mut last_dump = Instant::now();

    for message in consumer.start().wait() {
        if (Instant::now() - last_dump) > Duration::from_secs(60) {
            info!("Dumping local offset cache ({}: {} updates)", cluster_id, local_cache.len());
            update_global_cache(&cluster_id, &local_cache, &cache);
            local_cache = HashMap::with_capacity(local_cache.len());
            last_dump = Instant::now();
        }
        match message {
            Err(e) => {
                warn!("Can't receive data from stream: {:?}", e);
            },
            Ok(Ok(m)) => {
                let key = match m.key_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error while deserializing message key: {:?}", e);
                        &[]
                    },
                };
                let payload = match m.payload_view::<[u8]>() {
                    None => &[],
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        error!("Error while deserializing message payload: {:?}", e);
                        &[]
                    },
                };
                parse_message(key, payload);
                match parse_message(key, payload) {
                    Ok(update) => match update {
                        ConsumerUpdate::SetCommit {group, topic, partition, offset} => {
                            let mut offsets = local_cache.entry((group.to_owned(), topic.to_owned())).or_insert(Vec::new());
                            insert_at(&mut offsets, partition as usize, offset);
                        },
                        _ => {},
                    },
                    Err(e) => format_error_chain(e),
                };
            },
            Ok(Err(e)) => {
                warn!("Kafka error: {:?}", e);
            },
        };
    }
    Ok(())
}

pub fn run_offset_consumer(cluster_id: &ClusterId, brokers: &str, offset_cache: OffsetsCache) {
    let brokers_owned = brokers.to_owned();
    let mut consumer = create_consumer(brokers_owned);

    let mut cluster_id_clone = cluster_id.clone();
    thread::spawn(move || {
        consume_offset_topic(cluster_id_clone, consumer, offset_cache);
    });

    // let mut local_cache_clone = local_cache.clone();
    // thread::spawn(move || {
    //     loop {
    //         thread::sleep_ms(20000);
    //         // println!("> {:?}", local_cache_clone);
    //     }
    // });
}
