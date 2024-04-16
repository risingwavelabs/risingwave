#!/bin/bash

set -euo pipefail

# set gcloud
docker compose exec gcloud-cli gcloud auth login --cred-file=/gcp-rwctest.json

docker compose exec gcloud-cli gcloud config set project rwctest

bq_prepare_file='bq_prepare.sql'
bq_prepare_content=$(cat $bq_prepare_file)

docker compose exec gcloud-cli bq query --use_legacy_sql=false "$bq_prepare_content"

sleep 10
