# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.94.0
ARG RUST_IMAGE=rust:1.94.0-bookworm@sha256:365468470075493dc4583f47387001854321c5a8583ea9604b297e67f01c5a4f
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

FROM ${RUST_IMAGE} AS builder

WORKDIR /workspace
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release -p shardline --bin shardline \
    && cp /workspace/target/release/shardline /tmp/shardline

FROM ${RUNTIME_IMAGE} AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 shardline \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /var/lib/shardline shardline \
    && mkdir -p /var/lib/shardline \
    && chown -R shardline:shardline /var/lib/shardline

COPY --from=builder /tmp/shardline /usr/local/bin/shardline

USER 10001:10001
# Ensure the container can run with arbitrary non-root UIDs
ENV SHARDLINE_BIND_ADDR=0.0.0.0:8080
ENV SHARDLINE_PUBLIC_BASE_URL=http://127.0.0.1:8080
ENV SHARDLINE_ROOT_DIR=/var/lib/shardline
ENV SHARDLINE_CHUNK_SIZE_BYTES=65536

EXPOSE 8080
VOLUME ["/var/lib/shardline"]
STOPSIGNAL SIGINT

ENTRYPOINT ["/usr/local/bin/shardline"]
CMD ["serve"]
