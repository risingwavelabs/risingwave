# RisingWave Grafana Dashboard

The Grafana dashboard is generated with grafanalib. You'll need 

- Python
- grafanalib

  ```
  pip3 install grafanalib`
  ```

- jq: [instruction here](https://stedolan.github.io/jq/download/)

... preferably installed in a local venv.

And don't forget to include the generated `risingwave-dashboard.json` in the commit.

## Generate Dashboard

```
./generate.sh
```

## Update without Restarting Grafana

```
./update.sh
```

## Advanced Usage
We can specify the source uid, dashboard uid, dashboard version and enable namespace filter via env variables. 
For example, we can use the following query to generate dashboard json used in our benchmark cluster:
```
DASHBOARD_NAMESPACE_FILTER_ENABLED=true \
DASHBOARD_SOURCE_UID=<source_uid> \
DASHBOARD_UID=<dashboard_uid> \
DASHBOARD_VERSION=<version> \
./generate.sh
```
