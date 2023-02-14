FROM ubuntu:22.04 as base

ENV LANG en_US.utf8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl pkg-config bash lld

FROM base as builder

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY ./ /risingwave

ENV PATH /root/.cargo/bin/:$PATH

ENV IN_CONTAINER=1

ARG GIT_SHA
ENV GIT_SHA=$GIT_SHA

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "/risingwave/rust-toolchain") \
  && rustup component add rustfmt

RUN cargo fetch
RUN mkdir -p /risingwave/bin

RUN cargo build -p risingwave_cmd -p risingwave_cmd_all --release --features "static-link static-log-level" && \
  mv /risingwave/target/release/{frontend,compute-node,meta-node,compactor,risingwave} /risingwave/bin/ && \
  cargo clean
RUN for component in "risingwave" "compute-node" "meta-node" "frontend" "compactor"; do \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/${component}; \
  done

FROM ubuntu:22.04 as image-base
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates && rm -rf /var/lib/{apt,dpkg,cache,log}/

FROM image-base as risingwave
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/bin/risingwave /risingwave/bin/risingwave
# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE docker-playground
ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]
