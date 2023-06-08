FROM ubuntu:22.04 AS risingwave-build-env

ENV LANG en_US.utf8

# Use AWS ubuntu mirror
RUN sed -i 's|http://archive.ubuntu.com/ubuntu|http://us-east-2.ec2.archive.ubuntu.com/ubuntu/|g' /etc/apt/sources.list
RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl parallel python3 python3-pip \
    openssl libssl-dev libsasl2-dev libcurl4-openssl-dev pkg-config bash openjdk-11-jdk wget unzip git tmux lld postgresql-client kafkacat netcat mysql-client \
    maven zstd libzstd-dev -yy \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

SHELL ["/bin/bash", "-c"]

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY rust-toolchain rust-toolchain

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

ENV PATH /root/.cargo/bin/:$PATH

RUN rustup show

RUN curl -sSL "https://github.com/bufbuild/buf/releases/download/v1.4.0/buf-$(uname -s)-$(uname -m).tar.gz" | \
    tar -xvzf - -C /usr/local --strip-components 1

# install python dependencies
RUN pip3 install pyarrow pytest

# add required rustup components
RUN rustup component add rustfmt llvm-tools-preview clippy

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# install build tools
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall -y --no-symlinks cargo-llvm-cov cargo-nextest cargo-udeps cargo-hakari cargo-sort cargo-make cargo-cache cargo-audit \
    && cargo install sccache --locked \
    && cargo install --git https://github.com/risinglightdb/sqllogictest-rs --rev dbadddb --bin sqllogictest --locked \
    && cargo cache -a \
    && rm -rf "/root/.cargo/registry/index" \
    && rm -rf "/root/.cargo/registry/cache" \
    && rm -rf "/root/.cargo/git/db"
RUN cargo uninstall cargo-binstall
