// Copyright 2024 RisingWave Labs
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

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use anyhow::{anyhow, bail, Context, Result};
use itertools::Itertools;
use sqlx::{ConnectOptions, Database};
use tempfile::NamedTempFile;
use url::Url;

use super::{risingwave_cmd, ExecuteContext, Task};
use crate::util::{get_program_args, get_program_env_cmd, get_program_name, is_env_set};
use crate::{
    add_hummock_backend, add_tempo_endpoint, Application, HummockInMemoryStrategy, MetaBackend,
    MetaNodeConfig,
};

/// URL for connecting to the SQL meta store, retrieved from the env var `RISEDEV_SQL_ENDPOINT`.
/// If it is not set, a temporary sqlite file is created and used.
///
/// # Examples
///
/// - `mysql://root:my-secret-pw@127.0.0.1:3306/metastore`
/// - `postgresql://localhost:5432/metastore`
/// - `sqlite:///path/to/file.db`
/// - `sqlite::memory:`
fn sql_endpoint_from_env() -> String {
    static SQL_ENDPOINT: LazyLock<String> = LazyLock::new(|| {
        if let Ok(endpoint) = env::var("RISEDEV_SQL_ENDPOINT") {
            tracing::info!(
                "sql endpoint from env RISEDEV_SQL_ENDPOINT resolved to `{}`",
                endpoint
            );
            endpoint
        } else {
            let temp_path = NamedTempFile::with_suffix(".db").unwrap().into_temp_path();
            let temp_sqlite_endpoint = format!("sqlite://{}?mode=rwc", temp_path.to_string_lossy());
            tracing::warn!(
                "env RISEDEV_SQL_ENDPOINT not set, use temporary sqlite `{}`",
                temp_sqlite_endpoint
            );
            temp_sqlite_endpoint
        }
    });

    SQL_ENDPOINT.to_owned()
}

pub struct MetaNodeService {
    config: MetaNodeConfig,
}

impl MetaNodeService {
    pub fn new(config: MetaNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    /// Apply command args according to config
    pub fn apply_command_args(
        cmd: &mut Command,
        config: &MetaNodeConfig,
        hummock_in_memory_strategy: HummockInMemoryStrategy,
    ) -> Result<()> {
        cmd.arg("--listen-addr")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--advertise-addr")
            .arg(format!("{}:{}", config.address, config.port))
            .arg("--dashboard-host")
            .arg(format!(
                "{}:{}",
                config.listen_address, config.dashboard_port
            ));

        cmd.arg("--prometheus-host").arg(format!(
            "{}:{}",
            config.listen_address, config.exporter_port
        ));

        match config.provide_prometheus.as_ref().unwrap().as_slice() {
            [] => {}
            [prometheus] => {
                cmd.arg("--prometheus-endpoint")
                    .arg(format!("http://{}:{}", prometheus.address, prometheus.port));
            }
            _ => {
                return Err(anyhow!(
                    "unexpected prometheus config {:?}, only 1 instance is supported",
                    config.provide_prometheus
                ))
            }
        }

        let mut is_persistent_meta_store = false;

        match &config.meta_backend {
            MetaBackend::Memory => {
                cmd.arg("--backend")
                    .arg("sql")
                    .arg("--sql-endpoint")
                    .arg("sqlite::memory:");
            }
            MetaBackend::Sqlite => {
                let sqlite_config = config.provide_sqlite_backend.as_ref().unwrap();
                assert_eq!(sqlite_config.len(), 1);
                is_persistent_meta_store = true;

                let prefix_data = env::var("PREFIX_DATA")?;
                let file_path = PathBuf::from(&prefix_data)
                    .join(&sqlite_config[0].id)
                    .join(&sqlite_config[0].file);
                cmd.arg("--backend")
                    .arg("sqlite")
                    .arg("--sql-endpoint")
                    .arg(file_path);
            }
            MetaBackend::Postgres => {
                let pg_config = config.provide_postgres_backend.as_ref().unwrap();
                let pg_store_config = pg_config
                    .iter()
                    .filter(|c| c.application == Application::Metastore)
                    .exactly_one()
                    .expect("more than one or no pg store config found for metastore");
                is_persistent_meta_store = true;

                cmd.arg("--backend")
                    .arg("postgres")
                    .arg("--sql-endpoint")
                    .arg(format!(
                        "{}:{}",
                        pg_store_config.address, pg_store_config.port,
                    ))
                    .arg("--sql-username")
                    .arg(&pg_store_config.user)
                    .arg("--sql-password")
                    .arg(&pg_store_config.password)
                    .arg("--sql-database")
                    .arg(&pg_store_config.database);
            }
            MetaBackend::Mysql => {
                let mysql_config = config.provide_mysql_backend.as_ref().unwrap();
                let mysql_store_config = mysql_config
                    .iter()
                    .filter(|c| c.application == Application::Metastore)
                    .exactly_one()
                    .expect("more than one or no mysql store config found for metastore");
                is_persistent_meta_store = true;

                cmd.arg("--backend")
                    .arg("mysql")
                    .arg("--sql-endpoint")
                    .arg(format!(
                        "{}:{}",
                        mysql_store_config.address, mysql_store_config.port,
                    ))
                    .arg("--sql-username")
                    .arg(&mysql_store_config.user)
                    .arg("--sql-password")
                    .arg(&mysql_store_config.password)
                    .arg("--sql-database")
                    .arg(&mysql_store_config.database);
            }
            MetaBackend::Env => {
                let endpoint = sql_endpoint_from_env();
                is_persistent_meta_store = true;

                cmd.arg("--backend")
                    .arg("sql")
                    .arg("--sql-endpoint")
                    .arg(endpoint);
            }
        }

        let provide_minio = config.provide_minio.as_ref().unwrap();
        let provide_opendal = config.provide_opendal.as_ref().unwrap();
        let provide_aws_s3 = config.provide_aws_s3.as_ref().unwrap();

        let provide_compute_node = config.provide_compute_node.as_ref().unwrap();
        let provide_compactor = config.provide_compactor.as_ref().unwrap();

        let (is_shared_backend, is_persistent_backend) = match (
            config.enable_in_memory_kv_state_backend,
            provide_minio.as_slice(),
            provide_aws_s3.as_slice(),
            provide_opendal.as_slice(),
        ) {
            (true, [], [], []) => {
                cmd.arg("--state-store").arg("in-memory");
                (false, false)
            }
            (true, _, _, _) => {
                return Err(anyhow!(
                    "When `enable_in_memory_kv_state_backend` is enabled, no minio and aws-s3 should be provided.",
                ));
            }
            (_, provide_minio, provide_aws_s3, provide_opendal) => add_hummock_backend(
                &config.id,
                provide_opendal,
                provide_minio,
                provide_aws_s3,
                hummock_in_memory_strategy,
                cmd,
            )?,
        };

        if (provide_compute_node.len() > 1 || !provide_compactor.is_empty()) && !is_shared_backend {
            if config.enable_in_memory_kv_state_backend {
                // Using a non-shared backend with multiple compute nodes will be problematic for
                // state sharing like scaling. However, for distributed end-to-end tests with
                // in-memory state store, this is acceptable.
            } else {
                return Err(anyhow!(
                    "Hummock storage may behave incorrectly with in-memory backend for multiple compute-node or compactor-enabled configuration. Should use a shared backend (e.g. MinIO) instead. Consider adding `use: minio` in risedev config."
                ));
            }
        }

        let provide_compactor = config.provide_compactor.as_ref().unwrap();
        if is_shared_backend && provide_compactor.is_empty() {
            return Err(anyhow!(
                "When using a shared backend (minio, aws-s3, or shared in-memory with `risedev playground`), at least one compactor is required. Consider adding `use: compactor` in risedev config."
            ));
        }
        if is_persistent_meta_store && !is_persistent_backend {
            return Err(anyhow!(
                "When using a persistent meta store (sql), a persistent state store is required (e.g. minio, aws-s3, etc.)."
            ));
        }

        cmd.arg("--data-directory").arg("hummock_001");

        let provide_tempo = config.provide_tempo.as_ref().unwrap();
        add_tempo_endpoint(provide_tempo, cmd)?;

        Ok(())
    }
}

impl Task for MetaNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = risingwave_cmd("meta-node")?;

        if crate::util::is_enable_backtrace() {
            cmd.env("RUST_BACKTRACE", "1");
        }

        if crate::util::is_env_set("RISEDEV_ENABLE_PROFILE") {
            cmd.env(
                "RW_PROFILE_PATH",
                Path::new(&env::var("PREFIX_LOG")?).join(format!("profile-{}", self.id())),
            );
        }

        if crate::util::is_env_set("RISEDEV_ENABLE_HEAP_PROFILE") {
            // See https://linux.die.net/man/3/jemalloc for the descriptions of profiling options
            let conf = "prof:true,lg_prof_interval:32,lg_prof_sample:19,prof_prefix:meta-node";
            cmd.env("_RJEM_MALLOC_CONF", conf); // prefixed for macos
            cmd.env("MALLOC_CONF", conf); // unprefixed for linux
        }

        if crate::util::is_env_set("ENABLE_BUILD_RW_CONNECTOR") {
            let prefix_bin = env::var("PREFIX_BIN")?;
            cmd.env(
                "CONNECTOR_LIBS_PATH",
                Path::new(&prefix_bin).join("connector-node/libs/"),
            );
        }

        Self::apply_command_args(&mut cmd, &self.config, HummockInMemoryStrategy::Isolated)?;

        let prefix_config = env::var("PREFIX_CONFIG")?;
        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"));

        if let MetaBackend::Env = self.config.meta_backend {
            if is_env_set("RISEDEV_CLEAN_START") {
                ctx.pb.set_message("initializing meta store from env...");
                initialize_meta_store()?;
            }
        }

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
            writeln!(
                &mut ctx.log,
                "Please use the following parameters to start the meta:\n{}\n{} {}\n\n",
                get_program_env_cmd(&cmd),
                get_program_name(&cmd),
                get_program_args(&cmd)
            )?;
        }

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}

fn initialize_meta_store() -> Result<(), anyhow::Error> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let endpoint: Url = sql_endpoint_from_env()
        .parse()
        .context("invalid url for SQL endpoint")?;
    let scheme = endpoint.scheme();

    // Retrieve the database name to use for the meta store.
    // Modify the URL to establish a temporary connection to initialize that database.
    let (db, init_url) = if sqlx::Postgres::URL_SCHEMES.contains(&scheme) {
        let options = sqlx::postgres::PgConnectOptions::from_url(&endpoint)
            .context("invalid database url for Postgres meta backend")?;

        let db = options
            .get_database()
            .unwrap_or_else(|| options.get_username()) // PG defaults to username if no database is specified
            .to_owned();
        // https://www.postgresql.org/docs/current/manage-ag-templatedbs.html
        let init_options = options.database("template1");
        let init_url = init_options.to_url_lossy();

        (db, init_url)
    } else if sqlx::MySql::URL_SCHEMES.contains(&scheme) {
        let options = sqlx::mysql::MySqlConnectOptions::from_url(&endpoint)
            .context("invalid database url for MySQL meta backend")?;

        let db = options
            .get_database()
            .context("database not specified for MySQL meta backend")?
            .to_owned();
        // Effectively unset the database field when converting back to URL, meaning connect to no database.
        let init_options = options.database("");
        let init_url = init_options.to_url_lossy();

        (db, init_url)
    } else if sqlx::Sqlite::URL_SCHEMES.contains(&scheme) {
        // For SQLite, simply empty the file.
        let options = sqlx::sqlite::SqliteConnectOptions::from_url(&endpoint)
            .context("invalid database url for SQLite meta backend")?;

        if endpoint.as_str().contains(":memory:") || endpoint.as_str().contains("mode=memory") {
            // SQLite in-memory database does not need initialization.
        } else {
            let filename = options.get_filename();
            fs_err::write(filename, b"").context("failed to empty SQLite file")?;
        }

        return Ok(());
    } else {
        bail!("unsupported SQL scheme for meta backend: {}", scheme);
    };

    rt.block_on(async move {
        use sqlx::any::*;
        install_default_drivers();

        let options = sqlx::any::AnyConnectOptions::from_url(&init_url)?
            .log_statements(log::LevelFilter::Debug);

        let mut conn = options
            .connect()
            .await
            .context("failed to connect to a template database for meta store")?;

        // Intentionally not executing in a transaction because Postgres does not allow it.
        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS {};", db))
            .execute(&mut conn)
            .await?;
        sqlx::raw_sql(&format!("CREATE DATABASE {};", db))
            .execute(&mut conn)
            .await?;

        Ok::<_, anyhow::Error>(())
    })
    .context("failed to initialize database for meta store")?;

    Ok(())
}
