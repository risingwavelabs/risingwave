FROM ubuntu:22.04 AS base

ENV LANG en_US.utf8

RUN apt-get update \
  && apt-get -y install ca-certificates build-essential libsasl2-dev openjdk-11-jdk

FROM base AS rust-base

RUN apt-get -y install make cmake protobuf-compiler curl bash lld unzip

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

# TODO: build dashboard, instead of downloading
RUN curl -LO https://github.com/risingwavelabs/risingwave/archive/refs/heads/dashboard-artifact.zip
RUN unzip dashboard-artifact.zip && mkdir /risingwave && mv risingwave-dashboard-artifact /risingwave/ui && rm dashboard-artifact.zip

COPY rust-toolchain rust-toolchain

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup show \
  && rustup component add rustfmt

RUN cargo install --git https://github.com/xxchan/cargo-chef cargo-chef --locked --rev 11f9fed

FROM rust-base AS rust-planner

RUN mkdir -p /risingwave
WORKDIR /risingwave
COPY ./ /risingwave

RUN cargo chef prepare --recipe-path recipe.json

FROM rust-base AS rust-builder

RUN mkdir -p /risingwave
WORKDIR /risingwave

COPY --from=rust-planner /risingwave/recipe.json recipe.json

# Build dependencies - this can be cached if the dependencies don't change
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY ./ /risingwave
RUN cargo fetch
RUN cargo build -p risingwave_cmd_all --release --features "rw-static-link"
RUN mkdir -p /risingwave/bin && mv /risingwave/target/release/risingwave /risingwave/bin/ && \
  cp ./target/release/build/tikv-jemalloc-sys-*/out/build/bin/jeprof /risingwave/bin/ && \
  chmod +x /risingwave/bin/jeprof && \
  mkdir -p /risingwave/lib && cargo clean

# We use rust-maven-plugin to build java-binding. So it's FROM rust-base
FROM rust-base as java-builder

RUN apt-get -y install maven

RUN mkdir -p /risingwave
WORKDIR /risingwave/java

# TODO: cache dependencies for java before building

COPY ./src/java_binding /risingwave/java_binding
COPY Cargo.toml Cargo.lock /risingwave/
COPY ./java /risingwave/java

RUN mvn -B package -Dmaven.test.skip=true -Djava.binding.release=true
RUN mkdir -p /risingwave/bin/connector-node && \
    tar -zxvf /risingwave/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz -C /risingwave/bin/connector-node

FROM base AS runtime

LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave

RUN apt-get -y install gdb \
  && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN mkdir -p /risingwave/bin/connector-node && mkdir -p /risingwave/lib

COPY --from=rust-builder /risingwave/bin/risingwave /risingwave/bin/risingwave
COPY --from=java-builder /risingwave/bin/connector-node /risingwave/bin/connector-node
COPY --from=rust-builder /risingwave/ui /risingwave/ui
COPY --from=rust-builder /risingwave/bin/jeprof /usr/local/bin/jeprof

# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE docker-playground
# Set default dashboard UI to local path instead of github proxy
ENV RW_DASHBOARD_UI_PATH /risingwave/ui
ENV IN_CONTAINER=1

ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]
