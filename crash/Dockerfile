FROM ubuntu:22.04 AS base

ENV LANG en_US.utf8

RUN apt-get update \
    && apt-get -y install ca-certificates build-essential libsasl2-dev openjdk-11-jdk

FROM base AS builder

RUN apt-get update && apt-get -y install make cmake protobuf-compiler curl bash lld maven unzip

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN mkdir -p /crash

WORKDIR /crash

COPY ./crash /crash
ENV PATH /root/.cargo/bin/:$PATH

RUN rustup default stable

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
    && rustup set profile minimal \
    && rustup show \
    && rustup component add rustfmt

RUN cargo build

FROM base AS risingwave

LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave/

COPY --from=builder /crash/target/debug/crash /crash/target/debug/crash

ENTRYPOINT [ "/crash/target/debug/crash" ]

