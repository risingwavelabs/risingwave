FROM golang as builder

ADD . /datagen-src
RUN cd /datagen-src && gofmt -s -w . && go build

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -y && apt-get install -yy awscli

COPY --from=builder /datagen-src/datagen /
