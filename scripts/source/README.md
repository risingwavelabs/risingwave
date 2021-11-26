The contents of this folder are for the streaming e2e tests.

`start_kafka.sh` starts a single-node `zookeeper` and `kafka` cluster using `docker compose up -d`, and a resident `kafkacat` container for data dumping and other purposes.

For the files under *test_data*, `start_kafka.sh` creates each file name as a topic on kafka and pours the content into the topic via kafkacat

