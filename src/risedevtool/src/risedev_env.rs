// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::doc_markdown)] // RiseDev

use std::fmt::Write;
use std::process::Command;

use crate::{Application, HummockInMemoryStrategy, ServiceConfig, add_hummock_backend};

/// Generate environment variables (put in file `.risingwave/config/risedev-env`)
/// from the given service configurations to be used by future
/// RiseDev commands, like `risedev ctl` or `risedev psql` ().
pub fn generate_risedev_env(services: &Vec<ServiceConfig>) -> String {
    let mut env = String::new();
    for item in services {
        match item {
            ServiceConfig::ComputeNode(c) => {
                // RW_HUMMOCK_URL
                // If the cluster is launched without a shared storage, we will skip this.
                {
                    let mut cmd = Command::new("compute-node");
                    if add_hummock_backend(
                        "dummy",
                        c.provide_opendal.as_ref().unwrap(),
                        c.provide_minio.as_ref().unwrap(),
                        c.provide_aws_s3.as_ref().unwrap(),
                        c.provide_moat.as_ref().unwrap(),
                        HummockInMemoryStrategy::Disallowed,
                        &mut cmd,
                    )
                    .is_ok()
                    {
                        writeln!(
                            env,
                            "RW_HUMMOCK_URL=\"{}\"",
                            cmd.get_args().nth(1).unwrap().to_str().unwrap()
                        )
                        .unwrap();
                    }
                }

                // RW_META_ADDR
                {
                    let meta_node = &c.provide_meta_node.as_ref().unwrap()[0];
                    writeln!(
                        env,
                        "RW_META_ADDR=\"http://{}:{}\"",
                        meta_node.address, meta_node.port
                    )
                    .unwrap();
                }
            }
            ServiceConfig::Frontend(c) => {
                let listen_address = &c.listen_address;
                writeln!(
                    env,
                    "RISEDEV_RW_FRONTEND_LISTEN_ADDRESS=\"{listen_address}\"",
                )
                .unwrap();
                let port = &c.port;
                writeln!(env, "RISEDEV_RW_FRONTEND_PORT=\"{port}\"",).unwrap();
            }
            ServiceConfig::Kafka(c) => {
                let brokers = format!("{}:{}", c.address, c.port);
                writeln!(env, r#"RISEDEV_KAFKA_BOOTSTRAP_SERVERS="{brokers}""#,).unwrap();
                writeln!(env, r#"RISEDEV_KAFKA_WITH_OPTIONS_COMMON="connector='kafka',properties.bootstrap.server='{brokers}'""#).unwrap();
                writeln!(env, r#"RPK_BROKERS="{brokers}""#).unwrap();
            }
            ServiceConfig::SchemaRegistry(c) => {
                let url = format!("http://{}:{}", c.address, c.port);
                writeln!(env, r#"RISEDEV_SCHEMA_REGISTRY_URL="{url}""#,).unwrap();
                writeln!(env, r#"RPK_REGISTRY_HOSTS="{url}""#).unwrap();
            }
            ServiceConfig::Pulsar(c) => {
                // These 2 names are NOT defined by Pulsar, but by us.
                // The `pulsar-admin` CLI uses a `PULSAR_CLIENT_CONF` file with `brokerServiceUrl` and `webServiceUrl`
                // It may be used by our upcoming `PulsarCat` #21401
                writeln!(
                    env,
                    r#"PULSAR_BROKER_URL="pulsar://{}:{}""#,
                    c.address, c.broker_port
                )
                .unwrap();
                writeln!(
                    env,
                    r#"PULSAR_HTTP_URL="http://{}:{}""#,
                    c.address, c.http_port
                )
                .unwrap();
            }
            ServiceConfig::MySql(c) if c.application != Application::Metastore => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                // These envs are used by `mysql` cli.
                writeln!(env, r#"MYSQL_HOST="{host}""#,).unwrap();
                writeln!(env, r#"MYSQL_TCP_PORT="{port}""#,).unwrap();
                // Note: There's no env var for the username read by `mysql` cli. Here we set
                // `RISEDEV_MYSQL_USER`, which will be read by `e2e_test/commands/mysql` when
                // running `risedev slt`, as a wrapper of `mysql` cli.
                writeln!(env, r#"RISEDEV_MYSQL_USER="{user}""#,).unwrap();
                writeln!(env, r#"MYSQL_PWD="{password}""#,).unwrap();
                // Note: user and password are not included in the common WITH options.
                // It's expected to create another dedicated user for the source.
                writeln!(env, r#"RISEDEV_MYSQL_WITH_OPTIONS_COMMON="connector='mysql-cdc',hostname='{host}',port='{port}'""#,).unwrap();
            }
            ServiceConfig::Pubsub(c) => {
                let address = &c.address;
                let port = &c.port;
                writeln!(env, r#"PUBSUB_EMULATOR_HOST="{address}:{port}""#,).unwrap();
                writeln!(env, r#"RISEDEV_PUBSUB_WITH_OPTIONS_COMMON="connector='google_pubsub',pubsub.emulator_host='{address}:{port}'""#,).unwrap();
            }
            ServiceConfig::Postgres(c) => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                // These envs are used by `postgres` cli.
                writeln!(env, r#"PGHOST="{host}""#,).unwrap();
                writeln!(env, r#"PGPORT="{port}""#,).unwrap();
                writeln!(env, r#"PGUSER="{user}""#,).unwrap();
                writeln!(env, r#"PGPASSWORD="{password}""#,).unwrap();
                writeln!(env, r#"PGDATABASE="{database}""#,).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_POSTGRES_WITH_OPTIONS_COMMON="connector='postgres-cdc',hostname='{host}',port='{port}'""#,
                )
                .unwrap();
            }
            ServiceConfig::SqlServer(c) => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                // These envs are used by `sqlcmd`.
                writeln!(env, r#"SQLCMDSERVER="{host}""#,).unwrap();
                writeln!(env, r#"SQLCMDPORT="{port}""#,).unwrap();
                writeln!(env, r#"SQLCMDUSER="{user}""#,).unwrap();
                writeln!(env, r#"SQLCMDPASSWORD="{password}""#,).unwrap();
                writeln!(env, r#"SQLCMDDBNAME="{database}""#,).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_SQLSERVER_WITH_OPTIONS_COMMON="connector='sqlserver-cdc',hostname='{host}',port='{port}',username='{user}',password='{password}',database.name='{database}'""#,
                )
                .unwrap();
            }
            ServiceConfig::ClickHouse(c) => {
                let host = &c.address;
                let http_port = &c.http_port;
                let native_port = &c.native_port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                let http_url = format!("http://{host}:{http_port}");

                writeln!(env, r#"CLICKHOUSE_HOST="{host}""#).unwrap();
                writeln!(env, r#"CLICKHOUSE_PORT="{native_port}""#).unwrap();
                writeln!(env, r#"CLICKHOUSE_HTTP_PORT="{http_port}""#).unwrap();
                writeln!(env, r#"CLICKHOUSE_USER="{user}""#).unwrap();
                writeln!(env, r#"CLICKHOUSE_PASSWORD="{password}""#).unwrap();
                writeln!(env, r#"CLICKHOUSE_DATABASE="{database}""#).unwrap();
                writeln!(env, r#"RISEDEV_CLICKHOUSE_HTTP_URL="{http_url}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_CLICKHOUSE_WITH_OPTIONS_COMMON="connector='clickhouse',clickhouse.url='{http_url}',clickhouse.user='{user}',clickhouse.password='{password}',clickhouse.database='{database}'""#,
                )
                .unwrap();
            }
            ServiceConfig::Nats(c) => {
                let server_url = format!("nats://{}:{}", c.address, c.port);
                writeln!(env, r#"NATS_SERVER_URL="{server_url}""#).unwrap();
                writeln!(env, r#"RISEDEV_NATS_SERVER_URL="{server_url}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_NATS_WITH_OPTIONS_COMMON="connector='nats',server_url='{server_url}'""#,
                )
                .unwrap();
            }
            ServiceConfig::Mqtt(c) => {
                let url = format!("tcp://{}:{}", c.address, c.port);
                writeln!(env, r#"MQTT_URL="{url}""#).unwrap();
                writeln!(env, r#"RISEDEV_MQTT_URL="{url}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_MQTT_WITH_OPTIONS_COMMON="connector='mqtt',url='{url}'""#,
                )
                .unwrap();
            }
            ServiceConfig::Minio(c) => {
                let endpoint = format!("http://{}:{}", c.address, c.port);
                writeln!(env, r#"RISEDEV_MINIO_ENDPOINT="{endpoint}""#).unwrap();
                writeln!(env, r#"RISEDEV_MINIO_ACCESS_KEY="{0}""#, c.root_user).unwrap();
                writeln!(env, r#"RISEDEV_MINIO_SECRET_KEY="{0}""#, c.root_password).unwrap();

                // Defaults used by `e2e_test/sink/deltalake_rust_sink.slt`.
                writeln!(
                    env,
                    r#"RISEDEV_DELTALAKE_LOCATION="s3a://deltalake/deltalake-test""#
                )
                .unwrap();
                writeln!(env, r#"RISEDEV_DELTALAKE_S3_ENDPOINT="{endpoint}""#).unwrap();
                writeln!(env, r#"RISEDEV_DELTALAKE_S3_REGION="us-east-1""#).unwrap();
                writeln!(env, r#"RISEDEV_DELTALAKE_S3_ACCESS_KEY="{0}""#, c.root_user).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_DELTALAKE_S3_SECRET_KEY="{0}""#,
                    c.root_password
                )
                .unwrap();
            }
            ServiceConfig::MongoDb(c) => {
                let host = &c.address;
                let port = &c.port;
                let url = format!("mongodb://{host}:{port}/?replicaSet=rs0");
                writeln!(env, r#"MONGODB_HOST="{host}""#).unwrap();
                writeln!(env, r#"MONGODB_PORT="{port}""#).unwrap();
                writeln!(env, r#"MONGODB_URL="{url}""#).unwrap();
                if !c.user_managed {
                    writeln!(env, r#"MONGODB_CONTAINER="risedev-{}""#, c.id).unwrap();
                }
                writeln!(
                    env,
                    r#"RISEDEV_MONGODB_WITH_OPTIONS_COMMON="connector='mongodb',mongodb.url='{url}'""#,
                )
                .unwrap();
            }
            ServiceConfig::ElasticSearch(c) => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                let url = format!("http://{host}:{port}");
                writeln!(env, r#"ELASTICSEARCH_HOST="{host}""#).unwrap();
                writeln!(env, r#"ELASTICSEARCH_PORT="{port}""#).unwrap();
                writeln!(env, r#"ELASTICSEARCH_USER="{user}""#).unwrap();
                writeln!(env, r#"ELASTICSEARCH_PASSWORD="{password}""#).unwrap();
                writeln!(env, r#"RISEDEV_ELASTICSEARCH_URL="{url}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_ELASTICSEARCH_WITH_OPTIONS_COMMON="connector='elasticsearch',url='{url}',username='{user}',password='{password}'""#,
                )
                .unwrap();
            }
            ServiceConfig::OpenSearch(c) => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                let url = format!("http://{host}:{port}");
                writeln!(env, r#"OPENSEARCH_HOST="{host}""#).unwrap();
                writeln!(env, r#"OPENSEARCH_PORT="{port}""#).unwrap();
                writeln!(env, r#"OPENSEARCH_USER="{user}""#).unwrap();
                writeln!(env, r#"OPENSEARCH_PASSWORD="{password}""#).unwrap();
                writeln!(env, r#"RISEDEV_OPENSEARCH_URL="{url}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_OPENSEARCH_WITH_OPTIONS_COMMON="connector='opensearch',url='{url}',username='{user}',password='{password}'""#,
                )
                .unwrap();
            }
            ServiceConfig::Doris(c) => {
                let host = &c.address;
                let http_port = &c.http_port;
                let query_port = &c.query_port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                let http_url = format!("http://{host}:{http_port}");
                writeln!(env, r#"DORIS_HOST="{host}""#).unwrap();
                writeln!(env, r#"DORIS_HTTP_PORT="{http_port}""#).unwrap();
                writeln!(env, r#"DORIS_QUERY_PORT="{query_port}""#).unwrap();
                if !c.user_managed {
                    writeln!(env, r#"DORIS_CONTAINER="risedev-{}""#, c.id).unwrap();
                }
                writeln!(env, r#"DORIS_USER="{user}""#).unwrap();
                writeln!(env, r#"DORIS_PASSWORD="{password}""#).unwrap();
                writeln!(env, r#"DORIS_DATABASE="{database}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_DORIS_WITH_OPTIONS_COMMON="connector='doris',doris.url='{http_url}',doris.user='{user}',doris.password='{password}',doris.database='{database}'""#,
                )
                .unwrap();
            }
            ServiceConfig::Starrocks(c) => {
                let host = &c.address;
                let http_port = &c.http_port;
                let query_port = &c.query_port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                writeln!(env, r#"STARROCKS_HOST="{host}""#).unwrap();
                writeln!(env, r#"STARROCKS_HTTP_PORT="{http_port}""#).unwrap();
                writeln!(env, r#"STARROCKS_QUERY_PORT="{query_port}""#).unwrap();
                if !c.user_managed {
                    writeln!(env, r#"STARROCKS_CONTAINER="risedev-{}-fe""#, c.id).unwrap();
                }
                writeln!(env, r#"STARROCKS_USER="{user}""#).unwrap();
                writeln!(env, r#"STARROCKS_PASSWORD="{password}""#).unwrap();
                writeln!(env, r#"STARROCKS_DATABASE="{database}""#).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_STARROCKS_WITH_OPTIONS_COMMON="connector='starrocks',starrocks.host='{host}',starrocks.mysqlport='{query_port}',starrocks.httpport='{http_port}',starrocks.user='{user}',starrocks.password='{password}',starrocks.database='{database}'""#,
                )
                .unwrap();
            }
            ServiceConfig::MetaNode(meta_node_config) => {
                writeln!(
                    env,
                    r#"RISEDEV_RW_META_DASHBOARD_ADDR="http://{}:{}""#,
                    meta_node_config.address, meta_node_config.dashboard_port
                )
                .unwrap();
            }
            ServiceConfig::Lakekeeper(c) => {
                let base_url = format!("http://{}:{}", c.address, c.port);
                let catalog_url = format!("{}/catalog", base_url);
                writeln!(env, r#"RISEDEV_LAKEKEEPER_URL="{base_url}""#,).unwrap();
                writeln!(env, r#"LAKEKEEPER_CATALOG_URL="{catalog_url}""#,).unwrap();
                writeln!(env, r#"RISEDEV_LAKEKEEPER_WITH_OPTIONS_COMMON="connector='iceberg',catalog.type='rest',catalog.uri='{catalog_url}'""#,).unwrap();
            }
            _ => {}
        }
    }
    env
}
