FROM ubuntu:20.04
CMD bash

# Commands:
#   docker build -t risingwave-build-env .
#   docker tag risingwave-build-env:latest public.ecr.aws/x5u3w5h6/risingwave-build-env:latest
#   docker push public.ecr.aws/x5u3w5h6/risingwave-build-env:latest

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ca-certificates lsb-release wget sudo
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb
RUN apt-get install -y ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb
RUN sed -i'' -e 's,https://apache.bintray.com/,https://apache.jfrog.io/artifactory/,g' /etc/apt/sources.list.d/apache-arrow.sources
RUN apt-get update
RUN apt-get install -y curl ccache libparquet-dev libpq-dev libpqxx-dev libtbb-dev lcov
RUN apt-get install -y build-essential cmake git libboost-all-dev libjemalloc-dev valgrind
RUN apt-get install -y zstd liblz4-tool liblzo2-dev openjdk-8-jdk golang ninja-build
RUN apt-get install -y unzip zip postgresql-server-dev-all
