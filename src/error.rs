error_chain! {
    errors {
        PoisonError(action: String) {
            description("poison error")
            display("poison error while {}", action)
        }
    }
}
