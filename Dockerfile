FROM rust:1.87-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd -r -u 10001 appuser
COPY --from=builder /app/target/release/rust-ws-server /usr/local/bin/rust-ws-server
USER appuser
EXPOSE 8080
ENV WS_BIND_ADDR=0.0.0.0:8080
ENTRYPOINT ["/usr/local/bin/rust-ws-server"]
