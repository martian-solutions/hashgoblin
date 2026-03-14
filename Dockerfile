# ── Build stage ──────────────────────────────────────────────────────────────
FROM rust:1.86-slim AS builder

WORKDIR /build

# Cache dependencies before copying source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary.
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/hashgoblin /usr/local/bin/hashgoblin

# Mount a directory here to scan files and persist the database.
VOLUME ["/data"]
WORKDIR /data

ENTRYPOINT ["hashgoblin"]
CMD ["--help"]
