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

RUN cargo fetch
RUN cargo build --release

FROM ubuntu:20.04 as image-base
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install openssl libssl-dev && rm -rf /var/lib/{apt,dpkg,cache,log}/

FROM image-base as frontend-node
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/target/release/frontend-v2 /risingwave/bin/frontend-v2
EXPOSE 4566
ENTRYPOINT [ "/risingwave/bin/frontend-v2" ]

FROM image-base as compute-node
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/target/release/compute-node /risingwave/bin/compute-node
EXPOSE 5687
ENTRYPOINT [ "/risingwave/bin/compute-node" ]

FROM image-base as meta-node
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/target/release/meta-node /risingwave/bin/meta-node
EXPOSE 5690
ENTRYPOINT [ "/risingwave/bin/meta-node" ]

FROM image-base as compactor-node
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/target/release/compactor /risingwave/bin/compactor
EXPOSE 6660
ENTRYPOINT [ "/risingwave/bin/compactor" ]

FROM image-base as risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/target/release/risingwave /risingwave/bin/risingwave
ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]
