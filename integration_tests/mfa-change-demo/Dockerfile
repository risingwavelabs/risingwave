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
#RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
#RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
#RUN apt update
RUN apt install -y postgresql-client

ADD ./server/model/requirements.txt $WORK_DIR/model-pipreqs.txt
ADD ./generator/requirements.txt $WORK_DIR/generator-pipreqs.txt
RUN pip3 install -r $WORK_DIR/model-pipreqs.txt
RUN pip3 install -r $WORK_DIR/generator-pipreqs.txt

RUN apt install -y lsof curl openssl libssl-dev pkg-config build-essential
RUN apt install -y cmake librdkafka-dev

# `cargo build` included in ./build
ADD ./server $WORK_DIR/build/server
ADD ./simulator $WORK_DIR/build/simulator
RUN $HOME/.cargo/bin/cargo build --manifest-path $WORK_DIR/build/server/Cargo.toml --release
RUN $HOME/.cargo/bin/cargo build --manifest-path $WORK_DIR/build/simulator/Cargo.toml --release
RUN cp $WORK_DIR/build/server/target/release/server $WORK_DIR/feature-store-server
RUN cp $WORK_DIR/build/simulator/target/release/simulator $WORK_DIR/feature-store-simulator
RUN rm -rf $WORK_DIR/build

ADD ./server/model $WORK_DIR/server/model
ADD ./generator $WORK_DIR/generator
ADD ./taxi-start.sql $WORK_DIR/taxi-start.sql
ADD ./run $WORK_DIR/run

ENTRYPOINT ["bash"]

CMD ["./run"]