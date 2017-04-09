use metadata::ClusterId;

error_chain! {
    errors {
        PoisonError(action: String) {
            description("poison error")
            display("poison error while {}", action)
        }
        MissingConsumerError(cluster: ClusterId) {
            description("consumer is missing from cache")
            display("consumer is missing from cache for cluster {}", cluster)
        }
    }
}
