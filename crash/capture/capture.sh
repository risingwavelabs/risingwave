#!/bin/sh
set -e

# SIGTERM propagation to child process
_t() {
  echo "[$(date '+%F %T')] Caught SIGTERM signal!"
  kill -TERM "$child" 2>/dev/null
}

trap _t SIGTERM

# set kubernetes related variables
n_name="${NODE_NAME:-unknown}"
c_name="${CLUSTER_NAME:-unknown}"

echo "[$(date '+%F %T')] Monitoring ${LOCAL_PATH} on ${n_name}"

# launch inotifywait in the background
inotifywait -q -m /"${LOCAL_PATH}" -e close_write | while read path action file
do
  n="${S3_BUCKET}/${c_name}/${n_name}.${file}"
  aws s3 cp "${path}/${file}" "s3://${n}" --only-show-errors
  echo "[$(date '+%F %T')] [coredump] '${file}' has been uploaded to 's3://${n}'"
done &

# propagate bash signal to child
child=$!
wait "$child"

