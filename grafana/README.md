# RisingWave Grafana Dashboard

The Grafana dashboard is generated with grafanalib. You'll need 

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

And don't forget to include the generated `risingwave-<xxx>-dashboard.json` in the commit.

## Generate Dashboard

```bash
./generate.sh
```

## Update without Restarting Grafana

```bash
./update.sh
```

## Advanced Usage

We can specify the source uid, dashboard uid, dashboard version, enable namespace filter and enable risingwave_name filter(used in multi-cluster deployment) via env variables. 

For example, we can use the following query to generate dashboard json used in our benchmark cluster:

```bash
DASHBOARD_NAMESPACE_FILTER_ENABLED=true \
DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED=true \
DASHBOARD_SOURCE_UID=<source_uid> \
DASHBOARD_UID=<dashboard_uid> \
DASHBOARD_VERSION=<version> \
./generate.sh
```

If you want to use multi-prometheus as the variable:

```bash
DASHBOARD_NAMESPACE_FILTER_ENABLED=true \
DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED=true \
DASHBOARD_DYNAMIC_SOURCE=true \
DASHBOARD_UID=<dashboard_uid> \
DASHBOARD_VERSION=<version> \
./generate.sh
```
