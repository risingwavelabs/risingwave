export PUBSUB_EMULATOR_HOST=localhost:8900

curl -X PUT http://localhost:8900/v1/projects/demo/topics/test
curl -X PUT http://localhost:8900/v1/projects/demo/subscriptions/sub \
    -H 'content-type: application/json' \
    --data '{"topic":"projects/demo/topics/test"}'

curl -X GET http://localhost:8900/v1/projects/demo/topics
curl -X GET http://localhost:8900/v1/projects/demo/subscriptions
