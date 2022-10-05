FROM ubuntu:20.04
CMD bash

# Commands:
#   aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/x5u3w5h
#   docker build -t risingwave-build-env .
#   docker tag risingwave-build-env:latest public.ecr.aws/x5u3w5h6/risingwave-build-env:latest
#   docker push public.ecr.aws/x5u3w5h6/risingwave-build-env:latest

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -yy ca-certificates lsb-release wget
RUN apt-get install -yy curl lcov git unzip zip zlib1g-dev cmake build-essential
RUN apt-get install -yy pkg-config make golang psmisc libssl-dev libsasl2-dev
