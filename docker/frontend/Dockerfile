FROM openjdk:11.0.13-jdk-bullseye as risingwave_fe_builder

ENV LANG en_US.utf8
ENV RISINGWAVE_FE_HOME=/risingwave_fe
ENV RISINGWAVE_FE_BUILD_ENV=docker
ENV JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install git \
    && mkdir -p $RISINGWAVE_FE_HOME/proto $RISINGWAVE_FE_HOME/java

WORKDIR $RISINGWAVE_FE_HOME
COPY proto ./proto

WORKDIR $RISINGWAVE_FE_HOME/java
COPY java ./

RUN ./gradlew build

FROM openjdk:11.0.13-jdk-bullseye

RUN mkdir dir -p /risingwave_fe/conf /risingwave_fe/lib

COPY --from=risingwave_fe_builder /risingwave_fe/java/pgserver/build/libs/risingwave-fe-runnable.jar /risingwave_fe/lib
COPY --from=risingwave_fe_builder /risingwave_fe/java/pgserver/build/resources/main/server.properties /risingwave_fe/conf
COPY --from=risingwave_fe_builder /risingwave_fe/java/pgserver/build/resources/main/logback.xml  /risingwave_fe/conf

WORKDIR /risingwave_fe

ENV RISINGWAVE_FE_CONF=/risingwave_fe/conf
ENV RISINGWAVE_FE_LIB=/risingwave_fe/lib

EXPOSE 4567
