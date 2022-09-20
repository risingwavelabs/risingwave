FROM ubuntu:22.04 as builder

ENV LANG en_US.utf8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl pkg-config bash lld

SHELL ["/bin/bash", "-c"]

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y

RUN mkdir -p /risingwave

WORKDIR /risingwave

COPY ./ /risingwave

ENV PATH /root/.cargo/bin/:$PATH

# We need to add the `rustfmt` dependency, otherwise `risingwave_pb` will not compile
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "/risingwave/rust-toolchain") \
  && rustup component add rustfmt

# RUN cargo fetch
RUN mkdir -p /risingwave/bin

ARG simd_disabled=false

RUN if [ "$simd_disabled" == "true" ]; then \
    echo "Disabling SIMD build flags for risingwave" && \
    . scripts/cargo-config-disable-simd.sh; \
  fi

RUN cargo build -p risingwave_cmd -p risingwave_cmd_all --release --features static-link && \
  mv /risingwave/target/release/{frontend,compute-node,meta-node,compactor,risingwave} /risingwave/bin/ && \
  cargo clean
RUN objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/risingwave && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/frontend && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/compute-node && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/meta-node && \
  objcopy --compress-debug-sections=zlib-gnu /risingwave/bin/compactor

FROM ubuntu:22.04 as image-base
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates && rm -rf /var/lib/{apt,dpkg,cache,log}/

FROM image-base as frontend-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/bin/frontend /risingwave/bin/frontend
EXPOSE 4566
ENTRYPOINT [ "/risingwave/bin/frontend" ]

FROM image-base as compute-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/bin/compute-node /risingwave/bin/compute-node
EXPOSE 5687
ENTRYPOINT [ "/risingwave/bin/compute-node" ]

FROM image-base as meta-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/bin/meta-node /risingwave/bin/meta-node
EXPOSE 5690
ENTRYPOINT [ "/risingwave/bin/meta-node" ]

FROM image-base as compactor-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/bin/compactor /risingwave/bin/compactor
EXPOSE 6660
ENTRYPOINT [ "/risingwave/bin/compactor" ]

FROM image-base as risingwave
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder /risingwave/bin/risingwave /risingwave/bin/risingwave
# Copy risedev.yml into container
COPY ./risedev.yml /risedev.yml
# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE docker-playground
ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]
