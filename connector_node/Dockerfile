# USAGE: `cd <PROJECT_DIR> && docker build -t <IMAGE_NAME> -f Dockerfile`. 
# There is a built image in `https://github.com/risingwavelabs/risingwave/pkgs/container/risingwave-connector-node`.
# It's not recommended to build it yourself.

FROM maven:3.8.6-openjdk-11-slim AS builder

COPY . /code
WORKDIR /code

RUN mvn --no-transfer-progress -T 8 clean package -Dmaven.test.skip && cd /code/assembly/target \
 && mkdir tar-output && tar xf risingwave-connector-1.0.0.tar.gz -C tar-output

FROM openjdk:11

COPY --from=builder /code/assembly/target/tar-output /risingwave/bin/connector-node

ENTRYPOINT ["/risingwave/bin/connector-node/start-service.sh"]
