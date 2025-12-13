# Clarium (formerly Timeline) Rust service container

FROM rust:1.92-slim-bookworm AS build
LABEL authors="Euan MacInnes"

WORKDIR /app

# System deps for building (OpenSSL headers, pkg-config)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libssl-dev \
       pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Cache deps first
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
RUN mkdir -p src && echo "fn main() {}" > src/main.rs \
    && cargo build --release || true

# Real sources and scripts
COPY src/ src/
COPY scripts/ scripts/

# Build release binary (default features include pgwire)
RUN cargo build --release

FROM debian:bookworm-slim AS runtime

# Runtime deps (OpenSSL 3)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libssl3 \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/clarium

# Copy binary and scripts from builder
COPY --from=build /app/target/release/clarium /usr/local/bin/clarium
COPY --from=build /app/scripts/ ./scripts/

# Environment (retain compatibility with old TIMELINE_* vars)
ENV CLARIUM_DB_FOLDER=dbs \
    CLARIUM_HTTP_PORT=7878 \
    CLARIUM_PG_PORT=5433 \
    CLARIUM_PGWIRE=true

# Expose the default ports (HTTP 7878, PGWire 5433)
EXPOSE 7878 5433

# Default data directory inside the container
VOLUME ["/opt/clarium/dbs"]

ENTRYPOINT ["/usr/local/bin/clarium"]
CMD []
