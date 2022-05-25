FROM ubuntu:22.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install openssl libssl-dev && rm -rf /var/lib/{apt,dpkg,cache,log}/

RUN mkdir -p /risingwave/bin
COPY risingwave /risingwave/bin/risingwave
ENTRYPOINT [ "/risingwave/bin/risingwave" ]
CMD [ "playground" ]
