FROM ubuntu:24.04 AS base

ENV LANG=en_US.utf8

# Require Ubuntu's CVE-2026-114938 fix while allowing newer security updates.
# https://ubuntu.com/security/notices/USN-9156-1
RUN apt-get update \
  && apt-get -y install ca-certificates build-essential libsasl2-dev openjdk-21-jdk software-properties-common \
    python3.12 python3.12-dev openssl pkg-config curl git libblas-dev liblapack-dev libomp-dev \
  && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' openjdk-21-jre-headless)" ge '21.0.13.1+2-1~24.04.1' \
  && java -version

FROM base AS rust-base

RUN apt-get update && apt-get -y install make cmake protobuf-compiler bash lld unzip rsync

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH=/root/.cargo/bin:$PATH
ENV CARGO_INCREMENTAL=0

COPY rust-toolchain rust-toolchain

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup toolchain install --profile minimal \
  && rustup show \
  && rustup component add rustfmt \
  && rustup target add wasm32-wasip1

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
# RUN cargo chef cook --profile production --recipe-path recipe.json

FROM rust-base AS debug-tools

RUN cargo install flamegraph
RUN cargo install addr2line --features bin --bin addr2line

FROM rust-base AS rust-builder

# Only the Rust build needs Node.js for the embedded dashboard.
COPY dashboard/.node-version /tmp/dashboard-node-version
RUN set -eu; \
  node_version="$(cat /tmp/dashboard-node-version)"; \
  node_arch="$(dpkg --print-architecture)"; \
  if [ "$node_arch" = amd64 ]; then node_arch=x64; fi; \
  archive="node-v${node_version}-linux-${node_arch}.tar.gz"; \
  dist="https://nodejs.org/dist/v${node_version}"; \
  cd /tmp; \
  curl -fsSLO "$dist/$archive"; \
  curl -fsSLO "$dist/SHASUMS256.txt"; \
  grep -F "  $archive" SHASUMS256.txt | sha256sum -c -; \
  mkdir -p /opt/node; \
  tar -xzf "$archive" -C /opt/node --strip-components=1; \
  rm "$archive" SHASUMS256.txt
ENV PATH=/opt/node/bin:$PATH

COPY dashboard/package.json /tmp/dashboard-package.json
RUN npm install --global "$(node -p 'require("/tmp/dashboard-package.json").packageManager')"

# Build application
ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

ARG CARGO_PROFILE
ENV CARGO_PROFILE=$CARGO_PROFILE

COPY ./ /risingwave
WORKDIR /risingwave

ENV ENABLE_BUILD_DASHBOARD=1
ENV OPENSSL_STATIC=1

RUN cargo fetch && \
  cargo build -p risingwave_cmd_all --profile ${CARGO_PROFILE} --features "rw-static-link" --features udf --features datafusion --timings && \
  mkdir -p /risingwave/bin && \
  cp /risingwave/target/cargo-timings/cargo-timing.html /risingwave/bin/ && \
  mv /risingwave/target/${CARGO_PROFILE}/risingwave /risingwave/bin/ && \
  mv /risingwave/target/${CARGO_PROFILE}/risingwave.dwp /risingwave/bin/ && \
  cp ./target/${CARGO_PROFILE}/build/tikv-jemalloc-sys-*/out/build/bin/jeprof /risingwave/bin/ && \
  chmod +x /risingwave/bin/jeprof && \
  mkdir -p /risingwave/lib/adbc && \
  # Download ADBC Snowflake driver shared library for runtime (avoid invoking risedev/cargo-make here).
  DEST_DIR="/risingwave/lib/adbc" TMP_DIR="/tmp" bash ./src/risedevtool/scripts/download_adbc_snowflake_driver.sh && \
  cargo clean

FROM base AS java-planner

RUN mkdir -p /risingwave
WORKDIR /risingwave

COPY java /risingwave/java/

# Move java/**/pom.xml to poms/**/pom.xml
RUN find . -name pom.xml -exec bash -c 'mkdir -p poms/$(dirname {}); mv {} poms/{}' \;

# The Docker package uses the JNI bindings linked into the Rust binary.
FROM base AS java-builder

RUN apt-get update && apt-get -y install maven

RUN mkdir -p /risingwave
WORKDIR /risingwave/java

# 1. copy only poms
COPY --from=java-planner /risingwave/poms /risingwave/java/

# 2. start downloading dependencies
RUN mvn dependency:go-offline --fail-never

# 3. add the Java sources and the schemas used by the protobuf Maven plugin
COPY java /risingwave/java/
COPY proto /risingwave/proto/

RUN ./build-debezium.sh && \
  mvn -B package -Dmaven.test.skip=true -Dno-build-rust && \
  mkdir -p /risingwave/bin/connector-node && \
  tar -zxvf /risingwave/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz -C /risingwave/bin/connector-node

FROM base AS risingwave-nodwp

LABEL org.opencontainers.image.source=https://github.com/risingwavelabs/risingwave

RUN apt-get update && apt-get -y install linux-tools-generic \
  && ln -s "$(find /usr/lib/linux-tools/*/perf | head -1)" /usr/local/bin/perf

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install gdb libpam-krb5 krb5-user telnet kafkacat less rustup \
  && rm -rf /var/lib/{apt,dpkg,cache,log}/

# Set default Rust toolchain but don't install it to keep the image size small
# The toolchain will be installed when it is first used
# Do not use `rustup default stable` because it will install immediately
RUN rustup show && sed -i '1s/^/default_toolchain = "stable"\n/' ~/.rustup/settings.toml

RUN mkdir -p /risingwave/bin/connector-node && mkdir -p /risingwave/lib

COPY --from=rust-builder /risingwave/bin/risingwave /risingwave/bin/risingwave
COPY --from=java-builder /risingwave/bin/connector-node /risingwave/bin/connector-node
COPY --from=rust-builder /risingwave/bin/jeprof /usr/local/bin/jeprof
COPY --from=debug-tools /root/.cargo/bin/flamegraph /usr/local/bin/flamegraph
COPY --from=debug-tools /root/.cargo/bin/addr2line /usr/local/bin/addr2line

COPY --from=rust-builder /risingwave/lib/adbc /risingwave/lib/adbc

# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE=docker-playground
# Set default connector libs path
ENV CONNECTOR_LIBS_PATH=/risingwave/bin/connector-node/libs
ENV IN_CONTAINER=1

# Runtime dynamic library search path for ADBC (Linux containers use LD_LIBRARY_PATH).
ENV ADBC_DRIVER_PATH=/risingwave/lib/adbc
ENV LD_LIBRARY_PATH=/risingwave/lib/adbc

ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "single_node" ]

FROM risingwave-nodwp AS risingwave

COPY --from=rust-builder /risingwave/bin/risingwave.dwp /risingwave/bin/risingwave.dwp
COPY --from=rust-builder /risingwave/bin/cargo-timing.html /risingwave/bin/cargo-timing.html
