# Demo: Sinking to ElasticSearch

In this demo, we want to showcase how RisingWave is able to sink data to ElasticSearch.

1. Launch the cluster:

```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a single-node elasticsearch for sink.

2. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

3. Check the contents in ES:

```sh
# Check the document counts
curl -XGET -u elastic:risingwave "http://localhost:9200/test/_count" -H 'Content-Type: application/json'

# Check the content of a document by user_id
curl -XGET -u elastic:risingwave "http://localhost:9200/test/_search" -H 'Content-Type: application/json' -d '{"query":{"term": {"user_id":2}}' | jq

# Get the first 10 documents sort by user_id
curl -XGET -u elastic:risingwave "http://localhost:9200/test/_search?size=10" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}, "sort": ["user_id"]}' | jq
```
