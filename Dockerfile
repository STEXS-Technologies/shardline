# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.95.0

FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /workspace
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release -p shardline --bin shardline \
    && cp /workspace/target/release/shardline /tmp/shardline

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /var/lib/shardline shardline \
    && mkdir -p /var/lib/shardline \
    && chown -R shardline:shardline /var/lib/shardline

COPY --from=builder /tmp/shardline /usr/local/bin/shardline
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

USER root
ENV SHARDLINE_BIND_ADDR=0.0.0.0:8080
ENV SHARDLINE_PUBLIC_BASE_URL=http://127.0.0.1:8080
ENV SHARDLINE_ROOT_DIR=/var/lib/shardline
ENV SHARDLINE_CHUNK_SIZE_BYTES=65536

EXPOSE 8080
VOLUME ["/var/lib/shardline"]
STOPSIGNAL SIGINT
HEALTHCHECK --interval=10s --timeout=5s --retries=5 CMD ["shardline", "health", "--server", "http://127.0.0.1:8080"]

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["serve"]
