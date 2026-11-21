# taptap (github.com/litinoveweedle/taptap), built from source at a pinned commit.
# Only the TCP source is used: serialport's libudev default (port listing) is turned off,
# so it builds statically on musl. Cargo.lock still pins every other crate.
FROM rust:1.91-alpine3.22 AS taptap
RUN apk add --no-cache git musl-dev
ARG TAPTAP_REPO=https://github.com/litinoveweedle/taptap.git
ARG TAPTAP_COMMIT=71ec0ffd5ce84afbd714fbf39427991563481260
RUN git clone "$TAPTAP_REPO" /src && cd /src && git checkout --detach "$TAPTAP_COMMIT"
WORKDIR /src
RUN sed -i 's/^serialport = { version = "4.4", optional = true }$/serialport = { version = "4.4", optional = true, default-features = false }/' Cargo.toml \
 && grep -q 'default-features = false' Cargo.toml \
 && cargo build --release \
 && strip target/release/taptap

FROM python:3.13-alpine3.22
RUN pip install --no-cache-dir paho-mqtt==2.1.0 \
 && adduser -D -H -u 65532 tigo
COPY --from=taptap /src/target/release/taptap /usr/local/bin/taptap
COPY tap_exporter.py /app/tap_exporter.py
USER 65532
ENV STATE_DIR=/data PYTHONUNBUFFERED=1
EXPOSE 9812
ENTRYPOINT ["python3", "/app/tap_exporter.py"]
