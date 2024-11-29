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
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use console::style;
use fs_err::OpenOptions;
use indicatif::ProgressBar;
use risedev::util::{complete_spin, fail_spin};
use risedev::{
    generate_risedev_env, preflight_check, CompactorService, ComputeNodeService, ConfigExpander,
    ConfigureTmuxTask, DummyService, EnsureStopService, ExecuteContext, FrontendService,
    GrafanaService, KafkaService, MetaNodeService, MinioService, MySqlService, PostgresService,
    PrometheusService, PubsubService, RedisService, SchemaRegistryService, ServiceConfig,
    SqlServerService, SqliteConfig, Task, TempoService, RISEDEV_NAME,
};
use tempfile::tempdir;
use thiserror_ext::AsReport;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use yaml_rust::YamlEmitter;

#[derive(Default)]
pub struct ProgressManager {
    pa: Option<ProgressBar>,
}

impl ProgressManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new progress bar from task
    pub fn new_progress(&mut self) -> ProgressBar {
        if let Some(ref pa) = self.pa {
            pa.finish();
        }
        let pb = risedev::util::new_spinner();
        pb.enable_steady_tick(Duration::from_millis(100));
        self.pa = Some(pb.clone());
        pb
    }

    /// Finish all progress bars.
    pub fn finish_all(&self) {
        if let Some(ref pa) = self.pa {
            pa.finish();
        }
    }
}

fn task_main(
    manager: &mut ProgressManager,
    services: &Vec<ServiceConfig>,
    env: Vec<String>,
) -> Result<(Vec<(String, Duration)>, String)> {
    let log_path = env::var("PREFIX_LOG")?;

    let mut logger = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&log_path).join("risedev.log"))?;

    let status_dir = Arc::new(tempdir()?);

    let mut log_buffer = String::new();

    // Start Tmux and kill previous services
    {
        let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
        let mut service = ConfigureTmuxTask::new(env)?;
        service.execute(&mut ctx)?;

        writeln!(
            log_buffer,
            "* Run {} to attach to the tmux console.",
            style(format!("tmux -L {RISEDEV_NAME} a -t {RISEDEV_NAME}"))
                .blue()
                .bold()
        )?;
    }

    // Firstly, ensure that all ports needed is not occupied by previous runs.
    let mut ports = vec![];

    for service in services {
        if let Some(port) = service.port() {
            ports.push((port, service.id().to_string(), service.user_managed()));
        }
    }

    {
        let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
        let mut service = EnsureStopService::new(ports)?;
        service.execute(&mut ctx)?;
    }

    // Then, start services one by one

    let mut stat = vec![];

    for service in services {
        let start_time = Instant::now();

        match service {
            ServiceConfig::Minio(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = MinioService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task = risedev::ConfigureMinioTask::new(c.clone())?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::Sqlite(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());

                struct SqliteService(SqliteConfig);
                impl Task for SqliteService {
                    fn execute(
                        &mut self,
                        _ctx: &mut ExecuteContext<impl std::io::Write>,
                    ) -> anyhow::Result<()> {
                        Ok(())
                    }

                    fn id(&self) -> String {
                        self.0.id.clone()
                    }
                }

                let prefix_data = env::var("PREFIX_DATA")?;
                let file_dir = PathBuf::from(&prefix_data).join(&c.id);
                std::fs::create_dir_all(&file_dir)?;
                let file_path = file_dir.join(&c.file);

                ctx.service(&SqliteService(c.clone()));
                ctx.complete_spin();
                ctx.pb
                    .set_message(format!("using local sqlite: {:?}", file_path));
            }
            ServiceConfig::Prometheus(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = PrometheusService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, false)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api http://{}:{}/", c.address, c.port));
            }
            ServiceConfig::ComputeNode(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = ComputeNodeService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task =
                    risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api grpc://{}:{}/", c.address, c.port));
            }
            ServiceConfig::MetaNode(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = MetaNodeService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb.set_message(format!(
                    "api grpc://{}:{}/, dashboard http://{}:{}/",
                    c.address, c.port, c.address, c.dashboard_port
                ));
            }
            ServiceConfig::Frontend(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = FrontendService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api postgres://{}:{}/", c.address, c.port));

                writeln!(
                    log_buffer,
                    "* Run {} to start Postgres interactive shell.",
                    style(format_args!(
                        "psql -h localhost -p {} -d dev -U root",
                        c.port
                    ))
                    .blue()
                    .bold()
                )?;
            }
            ServiceConfig::Compactor(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = CompactorService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("compactor {}:{}", c.address, c.port));
            }
            ServiceConfig::Grafana(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = GrafanaService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, false)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("dashboard http://{}:{}/", c.address, c.port));
            }
            ServiceConfig::Tempo(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = TempoService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::TcpReadyCheckTask::new(c.listen_address.clone(), c.port, false)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api http://{}:{}/", c.listen_address, c.port));
            }
            ServiceConfig::AwsS3(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                DummyService::new(&c.id).execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("using AWS s3 bucket {}", c.bucket));
            }
            ServiceConfig::Opendal(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                DummyService::new(&c.id).execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("using Opendal, namenode =  {}", c.namenode));
            }
            ServiceConfig::Kafka(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = KafkaService::new(c.clone());
                service.execute(&mut ctx)?;
                let mut task = risedev::KafkaReadyCheckTask::new(c.clone())?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("kafka {}:{}", c.address, c.port));
            }
            ServiceConfig::SchemaRegistry(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = SchemaRegistryService::new(c.clone());
                service.execute(&mut ctx)?;
                if c.user_managed {
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                } else {
                    let mut task =
                        risedev::LogReadyCheckTask::new("Server started, listening for requests")?;
                    task.execute(&mut ctx)?;
                }
                ctx.pb
                    .set_message(format!("schema registry http://{}:{}", c.address, c.port));
            }

            ServiceConfig::Pubsub(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = PubsubService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::PubsubReadyTaskCheck::new(c.clone())?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("pubsub {}:{}", c.address, c.port));
            }
            ServiceConfig::RedPanda(_) => {
                return Err(anyhow!("redpanda is only supported in RiseDev compose."));
            }
            ServiceConfig::Redis(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = RedisService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::RedisReadyCheckTask::new(c.clone())?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("redis {}:{}", c.address, c.port));
            }
            ServiceConfig::MySql(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                MySqlService::new(c.clone()).execute(&mut ctx)?;
                if c.user_managed {
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                } else {
                    // When starting a MySQL container, the MySQL process is set as the main process.
                    // Since the first process in a container always gets PID 1, the MySQL log always shows
                    // "starting as process 1".
                    let mut task = risedev::LogReadyCheckTask::new("starting as process 1\n")?;
                    task.execute(&mut ctx)?;
                }
                ctx.pb
                    .set_message(format!("mysql {}:{}", c.address, c.port));
            }
            ServiceConfig::Postgres(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                PostgresService::new(c.clone()).execute(&mut ctx)?;
                if c.user_managed {
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                } else {
                    let mut task = risedev::LogReadyCheckTask::new_all([
                        "ready to accept connections", // also appears in init process
                        "listening on IPv4 address",   // only appears when ready
                    ])?;
                    task.execute(&mut ctx)?;
                }
                ctx.pb
                    .set_message(format!("postgres {}:{}", c.address, c.port));
            }
            ServiceConfig::SqlServer(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                // only `c.password` will be used in `SqlServerService` as the password for user `sa`.
                SqlServerService::new(c.clone()).execute(&mut ctx)?;
                if c.user_managed {
                    let mut task =
                        risedev::TcpReadyCheckTask::new(c.address.clone(), c.port, c.user_managed)?;
                    task.execute(&mut ctx)?;
                } else {
                    let mut task = risedev::LogReadyCheckTask::new(
                        "SQL Server is now ready for client connections.",
                    )?;
                    task.execute(&mut ctx)?;
                }
                ctx.pb
                    .set_message(format!("sqlserver {}:{}", c.address, c.port));
            }
        }

        let service_id = service.id().to_string();
        let duration = Instant::now() - start_time;
        stat.push((service_id, duration));
    }

    Ok((stat, log_buffer))
}

fn main() -> Result<()> {
    // Intentionally disable backtrace to provide more compact error message for `risedev dev`.
    // Backtraces for RisingWave components are enabled in `Task::execute`.
    std::env::set_var("RUST_BACKTRACE", "0");

    // Init logger from a customized env var.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var("RISEDEV_RUST_LOG")
                .from_env_lossy(),
        )
        .init();

    preflight_check()?;

    let task_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    let (config_path, env, risedev_config) = ConfigExpander::expand(".", &task_name)?;

    if let Some(config_path) = &config_path {
        let target = Path::new(&env::var("PREFIX_CONFIG")?).join("risingwave.toml");
        fs_err::copy(config_path, target).context("config file not found")?;
    }

    {
        let mut out_str = String::new();
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&risedev_config)?;
        fs_err::write(
            Path::new(&env::var("PREFIX_CONFIG")?).join("risedev-expanded.yml"),
            &out_str,
        )?;
    }
    let services = ConfigExpander::deserialize(&risedev_config)?;

    let mut manager = ProgressManager::new();
    // Always create a progress before calling `task_main`. Otherwise the progress bar won't be
    // shown.
    let p = manager.new_progress();
    p.set_prefix("dev cluster");
    p.set_message(format!(
        "starting {} services for {}...",
        services.len(),
        task_name
    ));
    let task_result = task_main(&mut manager, &services, env);

    match task_result {
        Ok(_) => {
            p.set_message(format!(
                "done bootstrapping with config {}",
                style(task_name).bold()
            ));
            complete_spin(&p);
        }
        Err(_) => {
            p.set_message(format!(
                "failed to bootstrap with config {}",
                style(task_name).bold()
            ));
            fail_spin(&p);
        }
    }
    manager.finish_all();

    use risedev::util::stylized_risedev_subcmd as r;

    match task_result {
        Ok((stat, log_buffer)) => {
            println!("---- summary of startup time ----");
            for (task_name, duration) in stat {
                println!("{}: {:.2}s", task_name, duration.as_secs_f64());
            }
            println!("-------------------------------");
            println!();

            fs_err::write(
                Path::new(&env::var("PREFIX_CONFIG")?).join("risedev-env"),
                generate_risedev_env(&services),
            )?;

            println!("All services started successfully.");

            print!("{}", log_buffer);

            println!("* You may find logs using {} command", r("l"));

            println!("* Run {} to kill cluster.", r("k"));

            println!("* Run {} to run `risedev` anywhere!", r("install"));

            Ok(())
        }
        Err(err) => {
            println!(
                "{} - Failed to start: {:#}", // pretty with `Caused by`
                style("ERROR").red().bold(),
                err.as_report(),
            );
            println!();
            println!(
                "* Use `{}` to enable new components, if they are missing.",
                r("configure")
            );
            println!(
                "* Use `{}` to view logs, or visit `{}`",
                r("l"),
                env::var("PREFIX_LOG")?
            );
            println!("* Run `{}` to clean up cluster.", r("k"));
            println!(
                "* Run `{}` to clean data, which might potentially fix the issue.",
                r("clean-data")
            );
            println!("---");
            println!();

            // As we have already printed the error above, we don't need to print that error again.
            // However, to return with a proper exit code, still return an error here.
            Err(anyhow!(
                "Failed to start all services. See details and instructions above."
            ))
        }
    }
}
