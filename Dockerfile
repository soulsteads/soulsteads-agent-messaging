FROM docker.io/library/rust:1.100.0-trixie@sha256:223cc32ddca8f3ee90c436dbc94b6b5f052ea7f9653c5201e9612f08ad7d21b8 AS build
WORKDIR /src
ENV CARGO_BUILD_JOBS=2
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY assets/ assets/
COPY tests/fixtures/acp.rs tests/fixtures/acp.rs
RUN cargo build --release --locked --bin soulsteads-messaging

FROM docker.io/library/debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
LABEL org.opencontainers.image.source="https://github.com/soulsteads/soulsteads-agent-messaging"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git libssl3t64 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --home-dir /data/home --no-create-home app \
    && install -d -m 0700 -o 10001 -g 10001 /data /data/home /data/sessions /data/workspace
COPY LICENSE NOTICE /usr/share/doc/soulsteads-agent-messaging/
COPY --from=build /src/target/release/soulsteads-messaging /usr/local/bin/soulsteads-messaging
ENV HOME=/data/home SESSION_BASE_DIR=/data/sessions
USER 10001:10001
WORKDIR /data
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/soulsteads-messaging"]
CMD ["--help"]
