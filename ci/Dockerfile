FROM ubuntu:22.04 AS risingwave-build-env

ENV LANG en_US.utf8

ARG RUST_TOOLCHAIN

RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive apt-get -y install make build-essential cmake protobuf-compiler curl parallel \
    openssl libssl-dev libsasl2-dev libcurl4-openssl-dev pkg-config bash openjdk-11-jdk wget unzip git tmux lld postgresql-client kafkacat netcat mysql-client \
    maven -yy \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/

SHELL ["/bin/bash", "-c"]

RUN echo export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64>>~/.bashrc
RUN echo source ~/.bashrc>>~/.bashrc
RUN wget "https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz"
RUN tar -zxvf hadoop-2.7.3.tar.gz -C /opt/
RUN mv /opt/hadoop-2.7.3 /opt/hadoop
RUN echo export HADOOP_HOME=/opt/hadoop/ >> ~/.bashrc
RUN echo export PATH=$PATH:$HADOOP_HOME/bin >> ~/.bashrc
RUN echo export PATH=$PATH:$HADOOP_HOME/sbin >> ~/.bashrc
RUN source ~/.bashrc   
RUN echo export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 >> /opt/hadoop/etc/hadoop/yarn-env.sh
RUN echo export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 >> /opt/hadoop/etc/hadoop/hadoop-env.sh

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain ${RUST_TOOLCHAIN} -y

RUN curl -sSL "https://github.com/bufbuild/buf/releases/download/v1.4.0/buf-$(uname -s)-$(uname -m).tar.gz" | \
    tar -xvzf - -C /usr/local --strip-components 1

RUN mkdir -p /risingwave

WORKDIR /risingwave

ENV PATH /root/.cargo/bin/:$PATH

# add required rustup components
RUN rustup component add rustfmt llvm-tools-preview clippy

# install build tools
RUN cargo install cargo-llvm-cov cargo-nextest cargo-udeps cargo-hakari cargo-sort cargo-make cargo-cache \
    && cargo install --git https://github.com/risinglightdb/sqllogictest-rs --rev dbadddb --bin sqllogictest --locked \
    && cargo cache -a \
    && rm -rf "/root/.cargo/registry/index" \
    && rm -rf "/root/.cargo/registry/cache" \
    && rm -rf "/root/.cargo/git/db"
