FROM ghcr.io/risingwavelabs/risingwave:latest as meta-last
USER root

RUN apt update
RUN apt install -y python3 python3-pip lsof

FROM rust:1.67 as feature-store-server

USER root

ENV WORK_DIR /opt/feature-store
RUN mkdir -p $WORK_DIR

WORKDIR $WORK_DIR

RUN apt update
RUN apt install -y python3 python3-pip wget ca-certificates
RUN apt install -y postgresql-client

ADD ./server/model/requirements.txt $WORK_DIR/model-pipreqs.txt
ADD ./generator/requirements.txt $WORK_DIR/generator-pipreqs.txt
RUN pip3 install -r $WORK_DIR/model-pipreqs.txt
RUN pip3 install -r $WORK_DIR/generator-pipreqs.txt

RUN apt install -y lsof curl openssl libssl-dev pkg-config build-essential
RUN apt install -y cmake librdkafka-dev

# Install .NET 6.0
RUN wget https://packages.microsoft.com/config/debian/11/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb
RUN apt-get update && apt-get install -y dotnet-sdk-6.0
RUN apt install -y liblttng-ust0

# `cargo build` included in ./build
ADD ./server $WORK_DIR/build/server
ADD ./simulator $WORK_DIR/build/simulator
RUN cargo build --manifest-path $WORK_DIR/build/server/Cargo.toml --release
RUN cargo build --manifest-path $WORK_DIR/build/simulator/Cargo.toml --release
# Use for MFA
# RUN cargo build --manifest-path $WORK_DIR/build/server/Cargo.toml --release -- --output-topics mfa
# RUN cargo build --manifest-path $WORK_DIR/build/simulator/Cargo.toml --release -- --types mfa
RUN cp $WORK_DIR/build/server/target/release/server $WORK_DIR/feature-store-server
RUN cp $WORK_DIR/build/simulator/target/release/simulator $WORK_DIR/feature-store-simulator
RUN rm -rf $WORK_DIR/build

ADD ./parquet_data.csv $WORK_DIR/parquet_data.csv
ADD ./server/model $WORK_DIR/server/model
ADD ./generator $WORK_DIR/generator
ADD ./taxi-start.sql $WORK_DIR/taxi-start.sql
ADD ./run.sh $WORK_DIR/run.sh
RUN chmod +x $WORK_DIR/run.sh

# Use for MFA
# CMD ["sh", "-c", "sleep 10 && ./run-mfa.sh"]
CMD ["sh", "-c", "sleep 10 && ./run.sh"]