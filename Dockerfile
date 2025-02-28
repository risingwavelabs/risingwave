FROM ubuntu:24.04 AS base

ENV LANG en_US.utf8

RUN apt-get update \
  && apt-get -y install ca-certificates build-essential libsasl2-dev openjdk-17-jdk software-properties-common python3.12 python3.12-dev openssl pkg-config curl

FROM base AS rust-base

RUN apt-get update && apt-get -y install make cmake protobuf-compiler bash lld unzip rsync

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin:$PATH
ENV CARGO_INCREMENTAL=0

COPY rust-toolchain rust-toolchain

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup show


FROM rust-base AS rust-builder

# Build application
ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

ARG CARGO_PROFILE
ENV CARGO_PROFILE=$CARGO_PROFILE

COPY ./ /risingwave
WORKDIR /risingwave

ENV ENABLE_BUILD_DASHBOARD=1
ENV OPENSSL_STATIC=1

RUN cargo clean && \
	cargo fetch && \
	cargo build --bin sink-bench --release
RUN mkdir -p /risingwave/bin
RUN cp ./target/release/sink-bench /risingwave/bin/
RUN cargo clean

ENTRYPOINT ["/risingwave/bin/sink-bench"]
