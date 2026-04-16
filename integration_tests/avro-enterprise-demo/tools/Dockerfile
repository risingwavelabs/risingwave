FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        jq \
        netcat-openbsd \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY tools/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace
