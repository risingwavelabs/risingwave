# Builder images: Only copy the binaries that are needed for the releavant target images to speed up build process
FROM rwk8scont as builder-compactor
COPY ./docker_target/release/compactor /risingwave/bin/compactor

FROM rwk8scont as builder-frontend
COPY ./docker_target/release/frontend /risingwave/bin/frontend

FROM rwk8scont as builder-compute-node
COPY ./docker_target/release/compute-node /risingwave/bin/compute-node

FROM rwk8scont as builder-meta-node
COPY ./docker_target/release/meta-node /risingwave/bin/meta-node

FROM rwk8scont as builder-risingwave
COPY ./docker_target/release/risingwave /risingwave/bin/risingwave




FROM ubuntu:22.04 as image-base
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates && rm -rf /var/lib/{apt,dpkg,cache,log}/

FROM image-base as frontend-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder-frontend /risingwave/bin/frontend /risingwave/bin/frontend
EXPOSE 4566
ENTRYPOINT [ "/risingwave/bin/frontend" ]

FROM image-base as compute-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder-compute-node /risingwave/bin/compute-node /risingwave/bin/compute-node
EXPOSE 5687
ENTRYPOINT [ "/risingwave/bin/compute-node" ]

FROM image-base as meta-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder-meta-node /risingwave/bin/meta-node /risingwave/bin/meta-node
EXPOSE 5690
ENTRYPOINT [ "/risingwave/bin/meta-node" ]

FROM image-base as compactor-node
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder-compactor /risingwave/bin/compactor /risingwave/bin/compactor
EXPOSE 6660
ENTRYPOINT [ "/risingwave/bin/compactor" ]

FROM image-base as risingwave
LABEL org.opencontainers.image.source https://github.com/risingwavelabs/risingwave
RUN mkdir -p /risingwave/bin
COPY --from=builder-risingwave /risingwave/bin/risingwave /risingwave/bin/risingwave
# Copy risedev.yml into container
COPY ./risedev.yml /risedev.yml
# Set default playground mode to docker-playground profile
ENV PLAYGROUND_PROFILE docker-playground
ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]


# TODO: use docker/Dockerfile by inserting new build layer into it. This way we do not have to copy-paste this part 

