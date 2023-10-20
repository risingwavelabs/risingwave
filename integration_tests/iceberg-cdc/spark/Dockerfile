FROM apache/spark:3.4.1 as build


USER root
RUN mkdir -p /home/spark/.m2 && chown -R spark:spark /home/spark

# Install Maven
USER spark

RUN mkdir -p /tmp/{maven,deps}
WORKDIR /tmp/maven
RUN wget --trust-server-names https://dlcdn.apache.org/maven/maven-3/3.9.4/binaries/apache-maven-3.9.4-bin.tar.gz
RUN tar -xzvf apache-maven-3.9.4-bin.tar.gz
COPY pom.xml /tmp/maven/pom.xml
RUN apache-maven-3.9.4/bin/mvn \
    -f /tmp/maven dependency:copy-dependencies -DoutputDirectory=/tmp/deps

FROM apache/spark:3.4.1

COPY --from=build /tmp/deps /opt/spark/deps
