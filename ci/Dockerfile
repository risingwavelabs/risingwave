FROM ubuntu:22.04 AS risingwave-build-env

ENV LANG en_US.utf8

ENV DEFAULT_RUST_TOOLCHAIN

RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl \
    openssl libssl-dev libcurl4-openssl-dev pkg-config bash openjdk-11-jdk wget unzip git tmux lld -yy

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain ${DEFAULT_RUST_TOOLCHAIN} -y

RUN curl -sSL "https://github.com/bufbuild/buf/releases/download/v1.4.0/buf-$(uname -s)-$(uname -m).tar.gz" | \
    tar -xvzf - -C /usr/local --strip-components 1

RUN mkdir -p /risingwave

WORKDIR /risingwave

ENV PATH /root/.cargo/bin/:$PATH

# install build tools
RUN cargo install cargo-llvm-cov cargo-nextest cargo-udeps cargo-hakari cargo-sort cargo-make

# pre-cache required crates
RUN git clone https://github.com/singularity-data/risingwave && cd risingwave && cargo fetch && cd .. && rm -rf risingwave
