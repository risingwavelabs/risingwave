FROM ghcr.io/singularity-data/risingwave:v0.1.11
MAINTAINER Kiv Chen <qiwen@singularity-data.com>
USER root

ENV WORK_DIR /opt/recwave
RUN mkdir -p $WORK_DIR

WORKDIR $WORK_DIR

RUN apt update
RUN apt install -y python3 python3-pip wget ca-certificates
#RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
#RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
#RUN apt update
RUN apt install -y postgresql-client

ADD ./recommender/model/requirements.txt $WORK_DIR/model-pipreqs.txt
ADD ./generator/requirements.txt $WORK_DIR/generator-pipreqs.txt
RUN pip3 install -r $WORK_DIR/model-pipreqs.txt
RUN pip3 install -r $WORK_DIR/generator-pipreqs.txt

RUN apt install openjdk-11-jdk -y
RUN wget https://dlcdn.apache.org/kafka/3.2.0/kafka_2.13-3.2.0.tgz
RUN tar -xvf kafka_2.13-3.2.0.tgz
RUN mv kafka_2.13-3.2.0 /usr/local/kafka

RUN apt install -y lsof curl openssl libssl-dev pkg-config build-essential
RUN apt install -y cmake librdkafka-dev

ENTRYPOINT ["bash"]

# CMD ["./recommender/run.sh", "./recommendatin"]