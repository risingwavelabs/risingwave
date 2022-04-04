FROM ubuntu:20.04 as builder

ENV LANG en_US.utf8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl openssl libssl-dev pkg-config

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY proto proto
COPY src src

ENV PATH /root/.cargo/bin/:$PATH

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "/risingwave/src/rust-toolchain") \
  && rustup component add rustfmt

RUN cd /risingwave/rust && cargo clean && cargo build --release

FROM alpine:latest

RUN mkdir -p /risingwave/bin

COPY --from=builder /risingwave/src/target/release/compute-node /risingwave/bin/compute-node
COPY --from=builder /risingwave/src/target/release/meta-node /risingwave/bin/meta-node

EXPOSE 5687 5690