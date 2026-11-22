FROM rust:1.85-slim-bookworm AS builder
WORKDIR /build
COPY solver/Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src
COPY solver/src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /build/target/release/solver /app/solver
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/solver
WORKDIR /app
CMD ["/app/run.sh"]
