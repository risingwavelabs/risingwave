# RisingWave Grafana Dashboard

The Grafana dashboard is generated with `grafanalib`. You'll need

- Python
- grafanalib
- jsonmerge
- jq: [instruction here](https://stedolan.github.io/jq/download/)

Preferably installed in a local Python virtual env (venv).

```bash
python3 -m venv 'venv'
source venv/bin/activate
pip install -r requirements.txt
```


## Generate Dashboard

```bash
./generate.sh
```

Don't forget to include the generated `risingwave-<xxx>-dashboard.json` in the commit.

## Update without Restarting Grafana (to localhost:3001)

```bash
./update.sh
```

## Multi-cluster Deployment

The `generate.sh` supports multi-cluster deployment. The following environment variables are helpful:

- `DASHBOARD_NAMESPACE_FILTER_ENABLED`: When set to `true`, a drop-down list will be added to the Grafana dashboard, and all Prometheus queries will be filtered by the selected namespace.
- `DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED`: When set to `true`, a drop-down list will be added to the Grafana dashboard, and all Prometheus queries will be filtered by the selected RisingWave name. This is useful when you have multiple RisingWave instances in the same namespace.
- `DASHBOARD_SOURCE_UID`: Set to the UID of your Prometheus source.
- `DASHBOARD_DYNAMIC_SOURCE`: Alternative to `DASHBOARD_SOURCE_UID`. When set to `true`, a drop-down list will be added to the Grafana dashboard to pick any one of the Prometheus sources.
- `DASHBOARD_UID`: Set to the UID of your Grafana dashboard.

See more details in the `common.py` file.

Examples:

```bash
DASHBOARD_NAMESPACE_FILTER_ENABLED=true \
DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED=true \
DASHBOARD_SOURCE_UID=<source_uid> \
DASHBOARD_UID=<dashboard_uid> \
./generate.sh
```

If you want to use multi-prometheus as the variable:

```bash
DASHBOARD_NAMESPACE_FILTER_ENABLED=true \
DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED=true \
DASHBOARD_DYNAMIC_SOURCE=true \
DASHBOARD_UID=<dashboard_uid> \
./generate.sh
```
