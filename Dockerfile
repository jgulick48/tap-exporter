# syntax=docker/dockerfile:1
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

FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder
ARG TARGETOS TARGETARCH VERSION=dev COMMIT=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /out/tap-exporter .

FROM alpine:3.22
RUN adduser -D -H -u 65532 tigo
COPY --from=taptap /src/target/release/taptap /usr/local/bin/taptap
COPY --from=builder /out/tap-exporter /usr/local/bin/tap-exporter
USER 65532
ENV STATE_DIR=/data
EXPOSE 9812
ENTRYPOINT ["/usr/local/bin/tap-exporter"]
