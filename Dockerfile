FROM rust:1.92-slim AS builder
WORKDIR /app

# Cache dependencies — touch a dummy main.rs so cargo can compile them.
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

# Build real binary (src changed → only this layer rebuilds).
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd -r -u 10001 appuser
COPY --from=builder /app/target/release/rust-ws-server /usr/local/bin/rust-ws-server
USER appuser
EXPOSE 8080
ENV WS_BIND_ADDR=0.0.0.0:8080
ENTRYPOINT ["/usr/local/bin/rust-ws-server"]
