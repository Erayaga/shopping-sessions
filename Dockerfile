# Shopping Sessions: native Rust pipeline.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
# Scorers are Intel Xeon Cascade Lake or newer: AVX2 (x86-64-v3) is safe.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked 2>/dev/null || cargo build --release; cp target/release/sessions /sessions

FROM debian:bookworm-slim
COPY --from=build /sessions /usr/local/bin/sessions
CMD ["/usr/local/bin/sessions"]
