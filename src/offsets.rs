use byteorder::{BigEndian, ReadBytesExt};
use rdkafka::config::{ClientConfig};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;

use rdkafka::{ Message, Offset, TopicPartitionList};
use rocket::futures::StreamExt;

use crate::cache::{Cache, OffsetsCache};
use crate::config::{ClusterConfig, Config};
use crate::error::*;
use crate::metadata::{ClusterId, TopicName};
use crate::utils::{insert_at, read_string};

use std::cmp;
use std::collections::HashMap;
use std::io::Cursor;
use std::str;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
enum ConsumerUpdate {
    Metadata,
    OffsetCommit {
        group: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    OffsetTombstone {
        group: String,
        topic: String,
        partition: i32,
    },
}

fn parse_group_offset(
    key_rdr: &mut Cursor<&[u8]>,
    payload_rdr: &mut Cursor<&[u8]>,
) -> Result<ConsumerUpdate> {
    let group = read_string(key_rdr).chain_err(|| "Failed to parse group name from key")?;
    let topic = read_string(key_rdr).chain_err(|| "Failed to parse topic name from key")?;
    let partition = key_rdr
        .read_i32::<BigEndian>()
        .chain_err(|| "Failed to parse partition from key")?;
    if !payload_rdr.get_ref().is_empty() {
        // payload is not empty
        let _version = payload_rdr
            .read_i16::<BigEndian>()
            .chain_err(|| "Failed to parse value version")?;
        let offset = payload_rdr
            .read_i64::<BigEndian>()
            .chain_err(|| "Failed to parse offset from value")?;
        Ok(ConsumerUpdate::OffsetCommit {
            group,
            topic,
            partition,
            offset,
        })
    } else {
        Ok(ConsumerUpdate::OffsetTombstone {
            group,
            topic,
            partition,
        })
    }
}

fn parse_message(key: &[u8], payload: &[u8]) -> Result<ConsumerUpdate> {
    let mut key_rdr = Cursor::new(key);
    let key_version = key_rdr
        .read_i16::<BigEndian>()
        .chain_err(|| "Failed to parse key version")?;
    match key_version {
        0 | 1 => parse_group_offset(&mut key_rdr, &mut Cursor::new(payload))
            .chain_err(|| "Failed to parse group offset update"),
        2 => Ok(ConsumerUpdate::Metadata),
        _ => bail!("Key version not recognized"),
    }
}

fn create_consumer(
    brokers: &str,
    group_id: &str,
    start_offsets: Option<Vec<i64>>,
    config:ClusterConfig
) -> Result<StreamConsumer<DefaultConsumerContext>> {

    info!("security_protocol={}",&config.security_protocol.clone().unwrap_or("".to_owned()) );
    let consumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("security.protocol", &config.security_protocol.clone().unwrap_or("".to_owned()) )
        .set("sasl.kerberos.service.name", &config.sasl_kerberos_service_name.clone().unwrap_or("".to_owned()) )
        .set("sasl.mechanism", &config.sasl_mechanism.clone().unwrap_or("".to_owned()) )
        .set("sasl.kerberos.principal", &config.sasl_kerberos_principal.clone().unwrap_or("".to_owned()) )
        .set("sasl.kerberos.keytab", &config.sasl_kerberos_keytab.clone().unwrap_or("".to_owned()) )
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "30000")
        .set("api.version.request", "true")
        //.set("fetch.message.max.bytes", "1024000") // Reduce memory usage
        .set("queued.min.messages", "10000") // Reduce memory usage
        .set("message.max.bytes", "10485760")
        .set("auto.offset.reset", "smallest")
        .create::<StreamConsumer<_>>()
        .chain_err(|| format!("Consumer creation failed: {}", brokers))?;

    match start_offsets {
        Some(pos) => {
            let mut tp_list = TopicPartitionList::new();
            for (partition, &offset) in pos.iter().enumerate() {
                tp_list.add_partition_offset(
                    "__consumer_offsets",
                    partition as i32,
                    Offset::Offset(offset),
                );
            }
            debug!(
                "Previous offsets found, assigning offsets explicitly: {:?}",
                tp_list
            );
            consumer
                .assign(&tp_list)
                .chain_err(|| "Failure during consumer assignment")?;
        }
        None => {
            debug!("No previous offsets found, subscribing to topic");
            consumer.subscribe(&["__consumer_offsets"]).chain_err(|| {
                format!("Can't subscribe to offset __consumer_offsets ({})", brokers)
            })?;
        }
    }

    Ok(consumer)
}

// we should really have some tests here
fn update_global_cache(
    cluster_id: &ClusterId,
    local_cache: &HashMap<(String, String), Vec<i64>>,
    cache: &OffsetsCache,
) {
    for (&(ref group, ref topic), new_offsets) in local_cache {
        // Consider a consuming iterator
        // This logic is not needed if i store the consumer offset, right? wrong!
        if new_offsets.iter().any(|&offset| offset == -1) {
            if let Some(mut existing_offsets) =
                cache.get(&(cluster_id.to_owned(), group.to_owned(), topic.to_owned()))
            {
                // If the new offset is not complete and i have an old one, do the merge
                vec_merge_in_place(&mut existing_offsets, &new_offsets, -1, cmp::max);
                //                for i in 0..(cmp::max(offsets.len(), existing_offsets.len())) {
                //                    let new_offset = cmp::max(
                //                        offsets.get(i).cloned().unwrap_or(-1),
                //                        existing_offsets.get(i).cloned().unwrap_or(-1));
                //                    insert_at(&mut existing_offsets, i, new_offset, -1);
                //                }
                let _ = cache.insert(
                    (cluster_id.to_owned(), group.to_owned(), topic.to_owned()),
                    existing_offsets,
                );
                continue;
            }
        }
        // TODO: log errors
        let _ = cache.insert(
            (cluster_id.to_owned(), group.to_owned(), topic.to_owned()),
            new_offsets.clone(),
        );
    }
}

fn commit_offset_position_to_array(tp_list: TopicPartitionList) -> Vec<i64> {
    let tp_elements = tp_list.elements_for_topic("__consumer_offsets");
    let mut offsets = vec![0; tp_elements.len()];
    for tp in &tp_elements {
        offsets[tp.partition() as usize] = tp.offset().to_raw().unwrap_or_default();
    }
    offsets
}

async fn consume_offset_topic(
    cluster_id: ClusterId,
    consumer: StreamConsumer<DefaultConsumerContext>,
    cache: &Cache,
) -> Result<()> {
    let mut local_cache = HashMap::new();
    let mut last_dump = Instant::now();

    debug!("Starting offset consumer loop for {:?}", cluster_id);

    let mut stream = consumer.stream();
    loop {
        let message = stream.next().await;
        match message {
            Some(Ok(m)) => {
                let om = m.detach();
                let key = om.key().unwrap_or(&[]);
                let payload = om.payload().unwrap_or(&[]);
                match parse_message(key, payload) {
                    Ok(ConsumerUpdate::OffsetCommit {
                        group,
                        topic,
                        partition,
                        offset,
                    }) => {
                        let mut offsets = local_cache
                            .entry((group.to_owned(), topic.to_owned()))
                            .or_insert_with(Vec::new);
                        insert_at(&mut offsets, partition as usize, offset, -1);
                    }
                    Ok(_) => {}
                    Err(e) => format_error_chain!(e),
                };
            }
            Some(Err(e)) => { warn!("Kafka error: {} {:?}", cluster_id, e);break;},
            None => {break;}
        };
        // Update the cache if needed
        if (Instant::now() - last_dump) > Duration::from_secs(10) {
            trace!(
                "Dumping local offset cache ({}: {} updates)",
                cluster_id,
                local_cache.len()
            );
            update_global_cache(&cluster_id, &local_cache, &cache.offsets);
            // Consumer position is not up to date after start, so we have to merge with the
            // existing offsets and take the largest.
            let res = consumer
                .position()
                .map(|current_position| {
                    let previous_position_vec = cache
                        .internal_offsets
                        .get(&cluster_id)
                        .unwrap_or_else(Vec::new);
                    let mut current_position_vec =
                        commit_offset_position_to_array(current_position);
                    vec_merge_in_place(
                        &mut current_position_vec,
                        &previous_position_vec,
                        Offset::Invalid.to_raw().unwrap_or_default(),
                        cmp::max,
                    );
                    cache
                        .internal_offsets
                        .insert(cluster_id.clone(), current_position_vec)
                })
                .chain_err(|| "Failed to store consumer offset position")?;
            if let Err(e) = res {
                format_error_chain!(e);
            }
            local_cache = HashMap::with_capacity(local_cache.len());
            last_dump = Instant::now();
        }
    }
    Ok(())
}

pub fn vec_merge_in_place<F, T: Copy>(vec1: &mut Vec<T>, vec2: &[T], default: T, merge_fn: F)
where
    F: Fn(T, T) -> T,
{
    let new_len = cmp::max(vec1.len(), vec2.len());
    vec1.resize(new_len, default);
    for i in 0..vec1.len() {
        vec1[i] = merge_fn(
            vec1.get(i).unwrap_or(&default).to_owned(),
            vec2.get(i).unwrap_or(&default).to_owned(),
        );
    }
}

//pub fn vec_merge<F, T: Clone>(vec1: Vec<T>, vec2: Vec<T>, default: T, merge: F) -> Vec<T>
//    where F: Fn(T, T) -> T
//{
//    (0..cmp::max(vec1.len(), vec2.len()))
//        .map(|index|
//            merge(
//                vec1.get(index).unwrap_or(&default).to_owned(),
//                vec2.get(index).unwrap_or(&default).to_owned()))
//        .collect::<Vec<T>>()
//}

pub   fn run_offset_consumer(
    cluster_id: &ClusterId,
    cluster_config: &ClusterConfig,
    config: &Config,
    cache: &Cache,
) -> Result<()> {
    let start_position = cache.internal_offsets.get(cluster_id);
    let consumer = create_consumer(
        &cluster_config.bootstrap_servers(),
        &config.consumer_offsets_group_id,
        start_position,cluster_config.clone()
    )
    .chain_err(|| format!("Failed to create offset consumer for {}", cluster_id))?;

    let cluster_id_clone = cluster_id.clone();
    let cache_alias = cache.alias();
    let _ = thread::Builder::new()
        .name("offset-consumer".to_owned())
        .spawn(|| {async move  {
            if let Err(e) = consume_offset_topic(cluster_id_clone, consumer, &cache_alias).await {
                format_error_chain!(e);
            }
        }})
        .chain_err(|| "Failed to start offset consumer thread")?;

    Ok(())
}

pub trait OffsetStore {
    fn offsets_by_cluster(
        &self,
        cluster_id: &ClusterId,
    ) -> Vec<((ClusterId, String, TopicName), Vec<i64>)>;
    fn offsets_by_cluster_topic(
        &self,
        cluster_id: &ClusterId,
        topic_name: &str,
    ) -> Vec<((ClusterId, String, TopicName), Vec<i64>)>;
    fn offsets_by_cluster_group(
        &self,
        cluster_id: &ClusterId,
        group_name: &str,
    ) -> Vec<((ClusterId, String, TopicName), Vec<i64>)>;
}

impl OffsetStore for Cache {
    fn offsets_by_cluster(
        &self,
        cluster: &ClusterId,
    ) -> Vec<((ClusterId, String, TopicName), Vec<i64>)> {
        self.offsets.filter_clone(|&(ref c, _, _)| c == cluster)
    }

    fn offsets_by_cluster_topic(
        &self,
        cluster: &ClusterId,
        topic: &str,
    ) -> Vec<((ClusterId, String, TopicName), Vec<i64>)> {
        self.offsets
            .filter_clone(|&(ref c, _, ref t)| c == cluster && t == topic)
    }

    fn offsets_by_cluster_group(
        &self,
        cluster: &ClusterId,
        group: &str,
    ) -> Vec<((ClusterId, String, TopicName), Vec<i64>)> {
        self.offsets
            .filter_clone(|&(ref c, ref g, _)| c == cluster && g == group)
    }
}
