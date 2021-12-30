The contents of this folder are for the streaming e2e tests.

`prepare_source.sh` starts a single-node `zookeeper` and `kafka` cluster using `docker compose up -d`, and a resident `kafkacat` container for data dumping and other purposes.

At the same time, `mysql` and `debezium` will be started and a corresponding sync will be created to facilitate testing of the cdc source.

For the files under *test_data*, `prepare_source.sh` creates each file name as a topic on kafka and pours the content into the topic via `kafkacat`


