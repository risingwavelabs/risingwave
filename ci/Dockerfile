FROM ubuntu:20.04 AS builder

ENV LANG en_US.utf8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl openssl libssl-dev pkg-config bash openjdk-11-jdk wget unzip git tmux

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY .git /risingwave/.git
COPY e2e_test /risingwave/e2e_test
COPY grafana /risingwave/grafana
COPY proto /risingwave/proto
COPY scripts /risingwave/scripts
COPY src /risingwave/src
COPY rust-toolchain /risingwave
COPY Cargo.toml /risingwave
COPY Makefile /risingwave
COPY Makefile.toml /risingwave
COPY risedev-components.ci.env /risingwave
COPY risedev.yml /risingwave

ENV PATH /root/.cargo/bin/:$PATH

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "/risingwave/rust-toolchain") \
  && rustup component add rustfmt

RUN cp risedev-components.ci.env risedev-components.user.env

RUN wget https://github.com/risinglightdb/sqllogictest-rs/releases/download/v0.3.4/sqllogictest-linux-amd64.tar.gz -O - | tar xz && \
    mv sqllogictest /usr/local/bin/sqllogictest && \
    chmod +x /usr/local/bin/sqllogictest

RUN curl -fL https://github.com/sagiegurari/cargo-make/releases/download/0.35.10/cargo-make-v0.35.10-x86_64-unknown-linux-musl.zip -o ~/cargo-make.zip && \
    unzip ~/cargo-make.zip -d ~ && \
    mv ~/cargo-make-v0.35.10-x86_64-unknown-linux-musl ~/cargo-make

RUN ~/cargo-make/makers pre-start-playground && \
    ~/cargo-make/makers link-all-in-one-binaries

FROM builder as risingwave-release
RUN cargo fetch
RUN cargo build -p risingwave_cmd_all -p risedev -p risingwave_regress_test --profile release

FROM builder as risingwave-dev
RUN cargo fetch
RUN cargo build -p risingwave_cmd_all -p risedev -p risingwave_regress_test --profile dev

