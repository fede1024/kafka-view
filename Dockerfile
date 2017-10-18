FROM messense/rust-musl-cross:x86_64-musl as builder
ADD . /home/rust/src
RUN apt-get update && apt-get install -y python && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN rustup update `cat rust-toolchain` && \
    rustup target add --toolchain `cat rust-toolchain` x86_64-unknown-linux-musl
RUN cargo build --release

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
RUN mkdir resources
COPY --from=builder /home/rust/src/resources ./resources
COPY --from=builder /home/rust/src/target/x86_64-unknown-linux-musl/release/kafka-view .
ENTRYPOINT ["./kafka-view"]
