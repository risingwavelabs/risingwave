FROM ubuntu:20.04 as builder

ENV LANG en_US.utf8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl openssl libssl-dev pkg-config

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY proto proto
COPY src src
COPY rust-toolchain rust-toolchain
COPY Cargo.toml Cargo.toml

ENV PATH /root/.cargo/bin/:$PATH

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "/risingwave/rust-toolchain") \
  && rustup component add rustfmt

RUN cargo clean && cargo build --release

FROM ubuntu:20.04

RUN mkdir -p /risingwave/bin

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install openssl libssl-dev

COPY --from=builder /risingwave/target/release/frontend-v2 /risingwave/bin/frontend-v2
COPY --from=builder /risingwave/target/release/compute-node /risingwave/bin/compute-node	
COPY --from=builder /risingwave/target/release/meta-node /risingwave/bin/meta-node

# Expose the necessary ports for the services:
# - 4566 is for frontend.
# - 5687 is for compute-node.
# - 5690 is for meta-node.
EXPOSE 4566 5687 5690
