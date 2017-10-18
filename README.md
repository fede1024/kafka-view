kafka-view
==========

[![Build Status](https://travis-ci.org/fede1024/kafka-view.svg?branch=master)](https://travis-ci.org/fede1024/kafka-view)
[![Docker Image](https://img.shields.io/docker/pulls/fede1024/kafka-view.svg?maxAge=2592000)](https://hub.docker.com/r/fede1024/kafka-view/)
[![Join the chat at https://gitter.im/rust-rdkafka/Lobby](https://badges.gitter.im/rust-rdkafka/Lobby.svg)](https://gitter.im/rust-rdkafka/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Kafka-view is an experimental web interface for Kafka written in Rust.
Kafka-view creates and maintains a materialized view of the internal state of
Kafka including cluster metadata, traffic metrics, group membership, consumer
offsets etc. It uses the [rdkafka](https://github.com/fede1024/rust-rdkafka)
Kafka client library for Rust, and [rocket](https://rocket.rs/).

Click [here](https://github.com/fede1024/kafka-view#screenshots) for screenshots.

Kafka-view supports multiple clusters and implements a fast search
functionality to quickly find a topic or consumer group by name or by regex,
across all clusters.

### Current features
* Available data:
  * Broker and topic metrics: byte rate and message rate for each broker and
    topic in every cluster.
  * Topic metadata: leader, replicas, ISR, topic health.
  * Group membership: show active consumer groups and members, easily find all
    the consumers for a given cluster or topic.
  * Consumer offsets: show the current consumer offsets, the high watermark and
    the difference between the two.
  * Consume topic content directly from the web UI.
* Search:
  * Omnisearch: search for broker, topics and consumers in a single query.
  * Search topics in all clusters by name or regex.
  * Search consumers in all clusters by name or regex.
  * Sort by any field (traffic, consumer lag, etc)

At the moment kafka-view is designed to be read-only. Functionality such as
adding topics, changing consumer offsets etc. are not supported.

## Configuring and running kafka-view

### Configuration

First, create a new configuration starting from the [example configuration file].
The new configuration should contain the list of clusters you want to monitor,
and a special topic in one of the clusters that kafka-view will use for caching.

The caching topic should be configured to use compaction. Example setup:

```bash
# Create topic
kafka-topics.sh --zookeeper <zk> --create --topic <cache_topic_name> --partitions 3 --replication-factor 2
# Enable compaction
kafka-topics.sh --zookeeper <zk> --alter --topic <cache_topic_name> --config cleanup.policy=compact
# Compact every 10MB per partition
kafka-topics.sh --zookeeper <zk> --alter --topic <cache_topic_name> --config segment.bytes=10485760
```

[example configuration file]: https://github.com/fede1024/kafka-view/blob/master/exampleConfig.yaml

### Building and running

To compile and run:
```bash
rustup override set $(cat rust-toolchain)
cargo run --release -- --conf config.yaml
```

To build Docker image and run(Assuming you have `config.yaml` in current working directory and set port to 8080 in it):
```bash
docker build -t kafka-view .
docker run --rm -p 8080:8080 -v `pwd`/config.yaml:/root/config.yaml kafka-view --conf config.yaml
```

Or you can use prebuilt image from Docker hub:
```bash
docker pull fede1024/kafka-view
docker run --rm -p 8080:8080 -v `pwd`/config.yaml:/root/config.yaml fede1024/kafka-view --conf config.yaml
```

### Metrics

Kafka exports metrics via JMX, which can be accessed via HTTP through [jolokia]. The suggested way
to run jolokia on your server is using the [JVM agent]. Example:

```bash
KAFKA_OPTS="-javaagent:jolokia-jvm-1.3.7-agent.jar=port=8778" ./bin/kafka-server-start.sh config/server.properties
```

To verify that it's correctly running:

```bash
curl http://localhost:8778/jolokia/read/java.lang:type=Memory/HeapMemoryUsage/used
```

Once your cluster is running with Jolokia, just add the jolokia port to the kafka-view configuration
and it will start reading metrics from the cluster.

[jolokia]: https://jolokia.org
[JVM agent]: https://jolokia.org/agent/jvm.html

## Implementation

### Information sources

* **Metadata**: cluster metadata is periodically polled using a background
  thread pool. Cluster metadata conatins: topic information (leader, replicas,
  ISR), broker information (broker id, hostname, etc), group membership (group
  state, members etc).
* **Metrics**: metrics such as byte rate and message rate per topic are polled
  in the background using a thread pool. Metrics are read using Jolokia, that
  mush be active on the Kafka brokers.
* **Consumer offsets**: Kafka-view consumes the `__consumer_offsets` topic and
  constantly receives the last offset commit for every consumer in every
  cluster.

### Data manipulation and storage

Every data is internally stored using a set of in-memory data structures
holding a normalized view of the last available value. When a web page is
loaded, the normalized data is combined together to generate the required
rapresentation of the data.

### Event caching

As a new update is received from the background polling threads or the
`__consumer_offsets` topics, a new event is created. Each event will update the
internal memory structures, and will also be periodically stored in a compacted
topic in Kafka. Kafka compaction will guarantee that the last update for every
key will be available on the topic.

When kafka-view restarts, the compacted topic is consumed and the internal
memory structures are restored to the previous state. In future version this
model will allow kafka-view to run in clustered mode, where multiple kafka-view
instances will work together to poll data from Kafka and will share the
information using the compacted topic.

## Contributors

Thanks to:
* [messense](https://github.com/messense)

## Screenshots

### Multiple cluster support

![clusters](/screenshots/clusters.png?raw=true "Clusters")

### Cluster level information

![combined](/screenshots/combined.png?raw=true "Cluster page")

### Consumer group information

![consumer](/screenshots/consumer.png?raw=true "Consumer group")


