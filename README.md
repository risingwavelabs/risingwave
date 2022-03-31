# RisingWave

[![Slack](https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack)](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)
[![CI](https://github.com/singularity-data/risingwave/actions/workflows/main.yml/badge.svg)](https://github.com/singularity-data/risingwave/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/singularity-data/risingwave/branch/main/graph/badge.svg?token=EB44K9K38B)](https://codecov.io/gh/singularity-data/risingwave)

## Quick Start

### Installation

You may start RisingWave with our pre-built binary, or build from source.

**Use Pre-built Binary**

```shell
wget <future risingwave binary URL>
./risingwave playground
```

**Build from Source**

```shell
git clone https://github.com/singularity-data/risingwave.git
./risedev dev  # start the cluster
./risedev kill # stop the cluster
```

Build from source requires several tools to be installed in the system. Refer to `CONTRIBUTING.md` for more information.

### The First Query

To connect to the RisingWave server, you will need to install Postgres client in advance.

```shell
psql -h localhost -p 4566 -d dev
```

```sql
/* create a table */
create table t1(v1 int not null);

/* create a materialized view based on the previous table */
create materialized view mv1 as select sum(v1) as sum_v1 from t1;

/* insert some data into the source table */
insert into t1 values (1), (2), (3);

/* ensure materialized view has been updated */
flush;

/* the materialized view should reflect the changes in source table */
select * from mv1;
```

If everything works, you will see

```
 sum_v1
--------
      6
(1 row)
```

in the Postgres shell.

## License

RisingWave is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.

## Contributing

Thanks for your interest in contributing to the project, please refer to [CONTRIBUTING.md](CONTRIBUTING.md) for more information.
