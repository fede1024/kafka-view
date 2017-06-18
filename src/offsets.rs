use byteorder::{BigEndian, ReadBytesExt};
use futures::Stream;
use rdkafka::{Message, TopicPartitionList, Offset};
use rdkafka::config::{ClientConfig, TopicConfig};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, EmptyConsumerContext};
use rdkafka::error::KafkaError;

use cache::{Cache, OffsetsCache};
use config::{Config, ClusterConfig};
use error::*;
use metadata::{ClusterId, TopicName};
use utils::insert_at;

use std::cmp;
use std::collections::HashMap;
use std::io::{Cursor, BufRead};
use std::str;
use std::thread;
use std::time::{Instant, Duration};


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

fn create_consumer(brokers: &str, group_id: &str, start_offsets: Option<Vec<i64>>) -> Result<StreamConsumer<EmptyConsumerContext>> {
    let consumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("fetch.message.max.bytes", "102400") // Reduce memory usage
        .set_default_topic_config(TopicConfig::new()
            .set("auto.offset.reset", "smallest")
            .finalize())
        .create::<StreamConsumer<_>>()
        .chain_err(|| format!("Consumer creation failed: {}", brokers))?;

    match start_offsets {
        Some(pos) => {
            let mut tp_list = TopicPartitionList::new();
            for (partition, &offset) in pos.iter().enumerate() {
                tp_list.add_partition_offset("__consumer_offsets", partition as i32, Offset::Offset(offset));
            }
            debug!("Previous offsets found, assigning offsets explicitly: {:?}", tp_list);
            consumer.assign(&tp_list)
                .chain_err(|| "Failure during consumer assignment")?;
        },
        None => {
            debug!("No previous offsets found, subscribing to topic");
            consumer.subscribe(&vec!["__consumer_offsets"])
                .chain_err(|| format!("Can't subscribe to offset __consumer_offsets ({})", brokers))?;
        }
    }

    Ok(consumer)
}

// we should really have some tests here
fn update_global_cache(cluster_id: &ClusterId, local_cache: &HashMap<(String, String), Vec<i64>>,
                       cache: &OffsetsCache) {
    for (&(ref group, ref topic), offsets) in local_cache {   // Consider a consuming iterator
        // This logic is not needed if i store the consumer offset, right? wrong!
        if offsets.iter().any(|&offset| offset == -1) {
            if let Some(mut existing_offsets) = cache.get(&(cluster_id.to_owned(), group.to_owned(), topic.to_owned())) {
                // If the new offset is not complete and i have an old one, do the merge
                for i in 0..(cmp::max(offsets.len(), existing_offsets.len())) {
                    let new_offset = cmp::max(
                        offsets.get(i).cloned().unwrap_or(-1),
                        existing_offsets.get(i).cloned().unwrap_or(-1));
                    insert_at(&mut existing_offsets, i, new_offset, -1);
                }
                cache.insert((cluster_id.to_owned(), group.to_owned(), topic.to_owned()), existing_offsets);
                continue;
            }
        }
        cache.insert((cluster_id.to_owned(), group.to_owned(), topic.to_owned()), offsets.clone());
    }
}

fn commit_offset_position_to_array(tp_list: TopicPartitionList) -> Vec<i64> {
    let tp_elements = tp_list.elements_for_topic("__consumer_offsets");
    let mut offsets = vec![0; tp_elements.len()];
    for tp in tp_elements.iter() {
        offsets[tp.partition() as usize] = tp.offset().to_raw();
    }
    offsets
}

fn consume_offset_topic(cluster_id: ClusterId, mut consumer: StreamConsumer<EmptyConsumerContext>,
                        cache: &Cache) -> Result<()> {
    let mut local_cache = HashMap::new();
    let mut last_dump = Instant::now();

    debug!("Starting offset consumer loop for {:?}", cluster_id);

    for message in consumer.start_with(Duration::from_millis(200), true).wait() {
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
                            insert_at(&mut offsets, partition as usize, offset, -1);
                        },
                        _ => {},
                    },
                    Err(e) => format_error_chain!(e),
                };
            },
            Ok(Err(KafkaError::NoMessageReceived)) => {
               continue;  // Jump to the end of the loop
            },
            Ok(Err(e)) => {
                warn!("Kafka error: {} {:?}", cluster_id, e);
            },
        };
        // Update the cache if needed
        if (Instant::now() - last_dump) > Duration::from_secs(10) {
            trace!("Dumping local offset cache ({}: {} updates)", cluster_id, local_cache.len());
            update_global_cache(&cluster_id, &local_cache, &cache.offsets);
            consumer.position()
                .map(|pos| {
                    let vec = commit_offset_position_to_array(pos);
                    trace!("Store consumer position: {:?}", vec);
                    cache.internal_offsets.insert(cluster_id.clone(), vec);
                });
            local_cache = HashMap::with_capacity(local_cache.len());
            last_dump = Instant::now();
        }
    }
    Ok(())
}

pub fn run_offset_consumer(cluster_id: &ClusterId, cluster_config: &ClusterConfig,
                           config: &Config, cache: &Cache) -> Result<()> {
    let start_position = cache.internal_offsets.get(&cluster_id);
    let consumer = create_consumer(&cluster_config.bootstrap_servers(), &config.consumer_offsets_group_id,
                                   start_position)
        .chain_err(|| format!("Failed to create offset consumer for {}", cluster_id))?;

    let cluster_id_clone = cluster_id.clone();
    let cache_alias = cache.alias();
    thread::Builder::new()
        .name("offset-consumer".to_owned())
        .spawn(move || {
            consume_offset_topic(cluster_id_clone, consumer, &cache_alias);
        });

    Ok(())
}


pub trait OffsetStore {
    fn offsets_by_cluster(&self, &ClusterId) -> Vec<((ClusterId, String, TopicName), Vec<i64>)>;
    fn offsets_by_cluster_topic(&self, &ClusterId, &TopicName) -> Vec<((ClusterId, String, TopicName), Vec<i64>)>;
    fn offsets_by_cluster_group(&self, &ClusterId, &String) -> Vec<((ClusterId, String, TopicName), Vec<i64>)>;
}

impl OffsetStore for Cache {
    fn offsets_by_cluster(&self, cluster: &ClusterId) -> Vec<((ClusterId, String, TopicName), Vec<i64>)> {
        self.offsets.filter_clone(|&(ref c, _, _)| c == cluster)
    }

    fn offsets_by_cluster_topic(&self, cluster: &ClusterId, topic: &TopicName) -> Vec<((ClusterId, String, TopicName), Vec<i64>)> {
        self.offsets.filter_clone(|&(ref c, _, ref t)| c == cluster && t == topic)
    }

    fn offsets_by_cluster_group(&self, cluster: &ClusterId, group: &String) -> Vec<((ClusterId, String, TopicName), Vec<i64>)> {
        self.offsets.filter_clone(|&(ref c, ref g, _)| c == cluster && g == group)
    }
}
