FROM ghcr.io/risingwavelabs/risingwave:latest as meta-last
USER root

RUN apt update
RUN apt install -y python3 python3-pip lsof

# CMD ["./recommender/run.sh", "./recommendatin"]