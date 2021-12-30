use std::collections::HashMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar};
use riselab::util::complete_spin;
use riselab::{
    ComputeNodeService, ConfigExpander, ConfigureTmuxTask, ExecuteContext, FrontendService,
    MetaNodeService, MinioService, PrometheusService, ServiceConfig, Task, RISELAB_SESSION_NAME,
};
use tempfile::tempdir;
use yaml_rust::YamlEmitter;

#[derive(Default)]
pub struct ProgressManager {
    mp: Arc<MultiProgress>,
    pa: Vec<ProgressBar>,
    insert: Option<usize>,
}

impl ProgressManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new progress bar from task
    pub fn new_progress(&mut self) -> ProgressBar {
        let pb = riselab::util::new_spinner();
        if let Some(ref mut insert) = self.insert {
            self.mp.insert(*insert, pb.clone());
            *insert += 1;
        } else {
            self.mp.add(pb.clone());
            self.insert = Some(0);
        }
        self.pa.push(pb.clone());
        pb.enable_steady_tick(100);
        pb
    }

    /// Finish all progress bars.
    pub fn finish_all(&self) {
        for p in &self.pa {
            p.finish();
        }
    }

    pub fn spawn(&self) -> JoinHandle<anyhow::Result<()>> {
        let mp = self.mp.clone();
        std::thread::spawn(move || mp.join().map_err(|err| err.into()))
    }
}

fn task_main(
    manager: &mut ProgressManager,
    steps: &[String],
    services: &HashMap<String, ServiceConfig>,
) -> Result<()> {
    let log_path = env::var("PREFIX_LOG")?;

    let mut logger = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&log_path).join("riselab.log"))?;

    let status_dir = Arc::new(tempdir()?);

    // Always start tmux first
    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
    let mut service = ConfigureTmuxTask::new()?;
    service.execute(&mut ctx)?;

    for step in steps {
        let service = services.get(step).unwrap();
        match service {
            ServiceConfig::Minio(_c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = MinioService::new()?;
                service.execute(&mut ctx)?;

                let mut task = riselab::ConfigureMinioTask::new()?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::Prometheus(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = PrometheusService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = riselab::ConfigureGrpcNodeTask::new(c.port, false)?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::ComputeNode(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = ComputeNodeService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task = riselab::ConfigureGrpcNodeTask::new(c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::MetaNode(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = MetaNodeService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task = riselab::ConfigureGrpcNodeTask::new(c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::Frontend(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = FrontendService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task = riselab::ConfigureGrpcNodeTask::new(c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let riselab_config = {
        let mut content = String::new();
        File::open("riselab.yml")?.read_to_string(&mut content)?;
        content
    };
    let riselab_config = ConfigExpander::expand(&riselab_config)?;
    {
        let mut out_str = String::new();
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&riselab_config)?;
        std::fs::write(
            Path::new(&env::var("PREFIX_CONFIG")?).join("riselab-expanded.yml"),
            &out_str,
        )?;
    }
    let task_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());
    let (steps, services) = ConfigExpander::select(&riselab_config, &task_name)?;

    let mut manager = ProgressManager::new();
    // Always create a progress before calling `task_main`. Otherwise the progress bar won't be
    // shown.
    let p = manager.new_progress();
    p.set_prefix("playground");
    p.set_message(format!(
        "starting {} services for {}...",
        steps.len(),
        task_name
    ));
    let join_handle = manager.spawn();
    let task_result = task_main(&mut manager, &steps, &services);
    p.set_message(format!("done {}", task_name));
    complete_spin(&p);
    manager.finish_all();
    join_handle.join().unwrap()?;

    let log_path = env::var("PREFIX_LOG")?;

    match &task_result {
        Ok(()) => {
            println!("All services started successfully.");

            println!("\nPRO TIPS:");
            println!(
                "* Run `tmux a -t {}` to attach to the tmux console.",
                RISELAB_SESSION_NAME
            );
            println!("* You may find logs at {}", log_path);
            println!(
                "* Run `psql -h localhost -p {} -d dev` to start Postgres interactive shell.",
                4567
            );
            println!("* Run `./riselab kill-playground` to kill cluster.");
        }
        Err(err) => {
            println!("* Failed to start: {}", err.root_cause().to_string().trim(),);
            println!(
                "please refer to logs for more information {}",
                env::var("PREFIX_LOG")?
            );
            println!("* Run `./riselab kill-playground` to clean up cluster.");
            println!("---");
            println!();
            println!();
        }
    }

    task_result
}
