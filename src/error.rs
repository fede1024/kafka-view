use crate::metadata::ClusterId;

error_chain! {
    errors {
        MissingConsumerError(cluster: ClusterId) {
            description("consumer is missing from cache")
            display("consumer is missing from cache for cluster {}", cluster)
        }
    }
}
