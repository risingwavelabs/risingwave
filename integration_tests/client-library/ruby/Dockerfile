FROM ruby:3.2-slim-bullseye

# install ruby-pg
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    apt-get install -y build-essential && \
    gem install pg

CMD ["irb"]
