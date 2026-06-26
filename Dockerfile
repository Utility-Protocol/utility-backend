FROM rust:slim AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential cmake libclang-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true

COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/utility-backend /usr/local/bin/utility-backend

EXPOSE 8443
CMD ["utility-backend"]
