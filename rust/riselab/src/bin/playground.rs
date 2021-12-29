use std::env;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar};
use riselab::{
    ComputeNodeService, ConfigureTmuxTask, ExecuteContext, FrontendService, MetaNodeService,
    MinioService, PrometheusService, Task, RISELAB_SESSION_NAME,
};
use tempfile::tempdir;

#[derive(Default)]
pub struct ProgressManager {
    mp: Arc<MultiProgress>,
    pa: Vec<ProgressBar>,
}

impl ProgressManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new progress bar from task
    pub fn new_progress(&mut self) -> ProgressBar {
        let pb = riselab::util::new_spinner();
        self.mp.add(pb.clone());
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

fn task_main(manager: &mut ProgressManager) -> Result<()> {
    let log_path = env::var("PREFIX_LOG")?;

    let mut logger = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&log_path).join("riselab.log"))?;

    let status_dir = Arc::new(tempdir()?);

    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
    let mut service = ConfigureTmuxTask::new()?;
    service.execute(&mut ctx)?;

    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
    let mut service = MinioService::new()?;
    service.execute(&mut ctx)?;

    let mut task = riselab::ConfigureMinioTask::new()?;
    task.execute(&mut ctx)?;

    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
    let mut service = PrometheusService::new(9500)?;
    service.execute(&mut ctx)?;
    let mut task = riselab::ConfigureGrpcNodeTask::new(9500)?;
    task.execute(&mut ctx)?;

    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
    let mut service = ComputeNodeService::new(5688)?;
    service.execute(&mut ctx)?;

    let mut task = riselab::ConfigureGrpcNodeTask::new(5688)?;
    task.execute(&mut ctx)?;

    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
    let mut service = MetaNodeService::new(5690)?;
    service.execute(&mut ctx)?;

    let mut task = riselab::ConfigureGrpcNodeTask::new(5690)?;
    task.execute(&mut ctx)?;

    let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir);
    let mut service = FrontendService::new()?;
    service.execute(&mut ctx)?;

    let postgres_port = 4567;
    let mut task = riselab::ConfigureGrpcNodeTask::new(postgres_port)?;
    task.execute(&mut ctx)?;
    Ok(())
}

fn main() -> Result<()> {
    let mut manager = ProgressManager::new();
    // Always create a progress before calling `task_main`. Otherwise the progress bar won't be
    // shown.
    let p = manager.new_progress();
    p.set_prefix("playground");
    p.set_message("starting services...");
    let join_handle = manager.spawn();
    let task_result = task_main(&mut manager);
    p.set_message("done");
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
