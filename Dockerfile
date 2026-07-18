FROM lukemathwalker/cargo-chef:latest-rust-1-slim AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    cmake \
    libclang-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
# Build and cache third-party dependencies
RUN cargo chef cook --release --recipe-path recipe.json

# Copy full application source and compile the backend
COPY . .
RUN cargo build --release

# Final runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/utility-backend /usr/local/bin/utility-backend

EXPOSE 8443
CMD ["utility-backend"]
