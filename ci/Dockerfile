FROM ubuntu:20.04 AS risingwave-build-env

ENV LANG en_US.utf8

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl \
    openssl libssl-dev libcurl4-openssl-dev pkg-config bash openjdk-11-jdk wget unzip git tmux lld

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN curl -sSL "https://github.com/bufbuild/buf/releases/download/v1.4.0/buf-$(uname -s)-$(uname -m).tar.gz" | \
    tar -xvzf - -C /usr/local --strip-components 1

RUN mkdir -p /risingwave

WORKDIR /risingwave

ENV PATH /root/.cargo/bin/:$PATH

RUN wget https://github.com/risinglightdb/sqllogictest-rs/releases/download/v0.4.0/sqllogictest-linux-amd64.tar.gz -O - | tar xz && \
    mv sqllogictest /usr/local/bin/sqllogictest && \
    chmod +x /usr/local/bin/sqllogictest

RUN curl -fL https://github.com/sagiegurari/cargo-make/releases/download/0.35.10/cargo-make-v0.35.10-x86_64-unknown-linux-musl.zip -o ~/cargo-make.zip && \
    unzip ~/cargo-make.zip -d ~ && \
    mv ~/cargo-make-v0.35.10-x86_64-unknown-linux-musl ~/cargo-make

RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin