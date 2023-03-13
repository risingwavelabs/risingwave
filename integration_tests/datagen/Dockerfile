FROM golang as builder

ADD . /datagen-src
RUN cd /datagen-src && gofmt -s -w . && go build

FROM ubuntu:20.04

COPY --from=builder /datagen-src/datagen /
