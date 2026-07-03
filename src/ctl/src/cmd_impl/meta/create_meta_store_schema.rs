// Copyright 2022 RisingWave Labs
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

use anyhow::Context;
use risingwave_common::config::{MetaBackend, MetaStoreConfig};
use risingwave_meta::MetaStoreBackend;
use risingwave_meta::controller::SqlMetaStore;

/// Options for `MetaCommands::CreateMetaStoreSchema`.
///
/// Mirrors the meta-store-selection subset of `RestoreOpts` so the command can
/// connect to the same backends supported by the meta node.
#[derive(clap::Args, Debug, Clone)]
pub struct CreateMetaStoreSchemaOpts {
    /// Type of meta store to apply schema changes to.
    #[clap(long, value_enum, default_value_t = MetaBackend::Mem)]
    pub meta_store_type: MetaBackend,
    /// Endpoint of the sql backend. For sqlite this is the file path; for
    /// postgres/mysql it is the `host[:port]` (username/password/database are
    /// passed separately).
    #[clap(long, default_value_t = String::from(""))]
    pub sql_endpoint: String,
    /// Username of sql backend, required when meta backend is set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_username: String,
    /// Password of sql backend, required when meta backend is set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_password: String,
    /// Database of sql backend, required when meta backend is set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_database: String,
    /// Extra URL parameters for the sql connection, e.g. `sslmode=disable`.
    /// Example: `param1=value1&param2=value2`.
    #[clap(long)]
    pub sql_url_params: Option<String>,
}

fn build_backend(opts: &CreateMetaStoreSchemaOpts) -> anyhow::Result<MetaStoreBackend> {
    let params_suffix = |params: &Option<String>| match params {
        Some(p) if !p.is_empty() => format!("?{p}"),
        _ => String::new(),
    };

    Ok(match opts.meta_store_type {
        MetaBackend::Mem => MetaStoreBackend::Mem,
        MetaBackend::Sql => MetaStoreBackend::Sql {
            endpoint: opts.sql_endpoint.clone(),
            config: MetaStoreConfig::default(),
        },
        MetaBackend::Sqlite => MetaStoreBackend::Sql {
            endpoint: format!("sqlite://{}?mode=rwc", opts.sql_endpoint),
            config: MetaStoreConfig::default(),
        },
        MetaBackend::Postgres => MetaStoreBackend::Sql {
            endpoint: format!(
                "postgres://{}:{}@{}/{}{}",
                opts.sql_username,
                opts.sql_password,
                opts.sql_endpoint,
                opts.sql_database,
                params_suffix(&opts.sql_url_params),
            ),
            config: MetaStoreConfig::default(),
        },
        MetaBackend::Mysql => MetaStoreBackend::Sql {
            endpoint: format!(
                "mysql://{}:{}@{}/{}{}",
                opts.sql_username,
                opts.sql_password,
                opts.sql_endpoint,
                opts.sql_database,
                params_suffix(&opts.sql_url_params),
            ),
            config: MetaStoreConfig::default(),
        },
    })
}

/// Apply all schema changes located under `src/meta/model/migration` to the
/// meta store, mirroring the behavior of `SqlMetaStore::up` without needing a
/// running meta node.
pub async fn create_meta_store_schema(opts: CreateMetaStoreSchemaOpts) -> anyhow::Result<()> {
    let backend = build_backend(&opts)?;
    let store = SqlMetaStore::connect(backend)
        .await
        .context("failed to connect to meta store")?;
    let first_launch = store
        .up()
        .await
        .context("failed to apply meta store schema")?;
    if first_launch {
        tracing::info!("meta store schema initialized (first launch)");
    } else {
        tracing::info!("meta store schema upgraded to the latest version");
    }
    Ok(())
}
