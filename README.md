kafka-view
==========

Kafka-view is an experimental web interface for Kafka written in Rust. Kafka-view creates and maintains a materialized view of the internal state of Kafka including cluster metadata, traffic metrics, group membership, consumer offsets etc. It uses the [rdkafka](https://github.com/fede1024/rust-rdkafka) Kafka client library for Rust.

Kafka-view supports multiple clusters and implements a fast search functionality to quickly find a topic or consumer group by name or by regex, across all clusters.

### Current features
* Available data:
  * Broker and topic metrics: byte rate and message rate for each broker and topic in every cluster.
  * Topic metadata: leader, replicas, ISR, topic health.
  * Group membership: show active consumer groups and members, easily find all the consumers for a given cluster or topic.
  * Consumer offsets: show the current consumer offsets, the high watermark and the difference between the two.
* Search:
  * Search topics in all clusters by name or regex.
  * Search consumers in all clusters by name or regex.
  * Sort by any field (traffic, consumer lag, etc)

### Coming features
* More metrics (topic size, traffic charts).
* Consume topic content directly from the web UI.
* Omnisearch: search for broker, topics and consumers in a single query.

At the moment kafka-view is designed to be read-only. Functionality such as adding topics, changing consumer offsets etc. are not supported and won't be supported in the near future.

Implementation
--------------

### Information sources

* **Metadata**: cluster metadata is periodically polled using a background thread pool. Cluster metadata conatins: topic information (leader, replicas, ISR), broker information (broker id, hostname, etc), group membership (group state, members etc).
* **Metrics**: metrics such as byte rate and message rate per topic are polled in the background using a thread pool. Metrics are read using Jolokia, that mush be active on the Kafka server.
* **Consumer offsets**: Kafka-view consumes the `__consumer_offsets` topic and constantly receives the last offset commit for every consumer in every cluster.

### Data manipulation and storage

Every data is internally stored using a set of in-memory data structures holding a normalized view of the last available value. When a web page is loaded, the normalized data is combined together to generate the required rapresentation of the data.

### Event caching

As a new update is received from the background polling threads or the `__consumer_offsets` topics, a new event is created. Each event will update the internal memory structures, and will also be stored in a compacted topic in Kafka. Kafka compaction will guarantee that the last update for every key will be available on the topic.

When kafka-view restarts, the compacted topic is consumed and the internal memory structures are restored to the previous state. In future version this model will allow kafka-view to run in clustered mode, where multiple kafka-view instances will work together to poll data from Kafka and will share the information using the compacted topic.
