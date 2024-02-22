FROM ubuntu:22.04 AS base

ENV LANG en_US.utf8

RUN apt-get update \
  && apt-get -y install ca-certificates build-essential libsasl2-dev openjdk-11-jdk

FROM base AS dashboard-builder

RUN apt-get update && apt-get install -y curl gnupg protobuf-compiler && mkdir -p /etc/apt/keyrings \
  && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
  && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list \
  && apt-get update && apt-get install -y nodejs

COPY ./dashboard/ /risingwave/dashboard
COPY ./proto  /risingwave/proto

RUN cd /risingwave/dashboard && npm i && npm run build-static && rm -rf node_modules

FROM base AS rust-base

RUN apt-get update && apt-get -y install make cmake protobuf-compiler curl bash lld unzip

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH
ENV CARGO_INCREMENTAL=0

COPY rust-toolchain rust-toolchain

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup show \
  && rustup component add rustfmt \
  && rustup target add wasm32-wasi

RUN cargo install flamegraph
# TODO: cargo-chef doesn't work well now, because we update Cargo.lock very often.
# We may consider sccache instead.

# RUN cargo install --git https://github.com/xxchan/cargo-chef cargo-chef --locked --rev 11f9fed

# FROM rust-base AS rust-planner

# RUN mkdir -p /risingwave
# WORKDIR /risingwave
# COPY ./ /risingwave

# RUN cargo chef prepare --recipe-path recipe.json

# FROM rust-base AS rust-builder

# RUN mkdir -p /risingwave
# WORKDIR /risingwave

# COPY --from=rust-planner /risingwave/recipe.json recipe.json

# # Build dependencies - this can be cached if the dependencies don't change
# RUN cargo chef cook --release --recipe-path recipe.json

FROM rust-base AS rust-builder

# Build application
ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

COPY ./ /risingwave
WORKDIR /risingwave

RUN cargo fetch && \
  cargo build -p risingwave_cmd_all --release --features "rw-static-link" && \
  mkdir -p /risingwave/bin && \
    mv /risingwave/target/release/risingwave /risingwave/bin/ && \
    mv /risingwave/target/release/risingwave.dwp /risingwave/bin/ && \
  cp ./target/release/build/tikv-jemalloc-sys-*/out/build/bin/jeprof /risingwave/bin/ && \
  chmod +x /risingwave/bin/jeprof && \
  mkdir -p /risingwave/lib && cargo clean

FROM base AS java-planner

RUN mkdir -p /risingwave
WORKDIR /risingwave

COPY java /risingwave/java/

# Move java/**/pom.xml to poms/**/pom.xml
RUN find . -name pom.xml -exec bash -c 'mkdir -p poms/$(dirname {}); mv {} poms/{}' \;

# We use rust-maven-plugin to build java-binding. So it's FROM rust-base
FROM rust-base AS java-builder

RUN apt-get update && apt-get -y install maven

RUN mkdir -p /risingwave
WORKDIR /risingwave/java

# 1. copy only poms
COPY --from=java-planner /risingwave/poms /risingwave/java/

# 2. start downloading dependencies
RUN mvn dependency:go-offline --fail-never

# 3. add all source code and start compiling
# TODO: only add java related code so that changing rust code won't recompile java code
# Currently java-binding depends on the workspace Cargo.toml, which depends on the whole rust codebase
# Besides, rust-maven-plugin sets --target-dir, so the dependencies are built twice. How to dedup?
COPY ./ /risingwave

RUN mvn -B package -Dmaven.test.skip=true -Dno-build-rust && \
  mkdir -p /risingwave/bin/connector-node && \
  tar -zxvf /risingwave/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz -C /risingwave/bin/connector-node

FROM base AS risingwave

LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave

RUN apt-get update && apt-get -y install linux-tools-generic \
    && ln -s "$(find /usr/lib/linux-tools/*/perf | head -1)" /usr/local/bin/perf

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install gdb libpam-krb5 krb5-user telnet kafkacat \
  && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN mkdir -p /risingwave/bin/connector-node && mkdir -p /risingwave/lib

COPY --from=rust-builder /risingwave/bin/risingwave /risingwave/bin/risingwave
COPY --from=rust-builder /risingwave/bin/risingwave.dwp /risingwave/bin/risingwave.dwp
COPY --from=java-builder /risingwave/bin/connector-node /risingwave/bin/connector-node
COPY --from=dashboard-builder /risingwave/dashboard/out /risingwave/ui
COPY --from=rust-builder /risingwave/bin/jeprof /usr/local/bin/jeprof
COPY --from=rust-base /root/.cargo/bin/flamegraph /usr/local/bin/flamegraph

# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE docker-playground
# Set default dashboard UI to local path instead of github proxy
ENV RW_DASHBOARD_UI_PATH /risingwave/ui
# Set default connector libs path
ENV CONNECTOR_LIBS_PATH /risingwave/bin/connector-node/libs
ENV IN_CONTAINER=1

ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]
