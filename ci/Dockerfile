FROM ubuntu:22.04 AS risingwave-build-env

ENV LANG en_US.utf8

ARG RUST_TOOLCHAIN

RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl parallel \
    openssl libssl-dev libsasl2-dev libcurl4-openssl-dev pkg-config bash openjdk-18-jdk wget unzip git tmux lld postgresql-client kafkacat \
    maven -yy \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain ${RUST_TOOLCHAIN} -y

RUN curl -sSL "https://github.com/bufbuild/buf/releases/download/v1.4.0/buf-$(uname -s)-$(uname -m).tar.gz" | \
    tar -xvzf - -C /usr/local --strip-components 1

RUN mkdir -p /risingwave

WORKDIR /risingwave

ENV PATH /root/.cargo/bin/:$PATH

# add required rustup components
RUN rustup component add rustfmt llvm-tools-preview clippy

# install sqllogictest-jdbc-driver
RUN curl -sSL "https://github.com/risinglightdb/sqllogictest-driver-jdbc/releases/download/v1.0.5/sqllogictest-jdbc-runner-1.0.5-jar-with-dependencies.jar" -o sqllogictest-jdbc-runner.jar

# install build tools
RUN cargo install cargo-llvm-cov cargo-nextest cargo-udeps cargo-hakari cargo-sort cargo-make cargo-cache \
    && cargo install --git https://github.com/risinglightdb/sqllogictest-rs --rev 879204b9b2dd5a81db4a9781c2e6c68d0713f5d3 --bin sqllogictest \
    && cargo cache -a \
    && rm -rf "/root/.cargo/registry/index" \
    && rm -rf "/root/.cargo/registry/cache" \
    && rm -rf "/root/.cargo/git/db"
