use std::env;
use std::fs::OpenOptions;
use std::path::Path;

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar};
use riselab::{
    ComputeNodeService, ConfigureTmuxTask, FrontendService, MetaNodeService, MinioService,
    RISELAB_SESSION_NAME,
};

fn create_multi_progress(progress: &[ProgressBar]) -> MultiProgress {
    let multi_progress = MultiProgress::new();
    for p in progress {
        multi_progress.add(p.clone());
    }
    multi_progress
}

fn finish_multi_progress(progress: &[ProgressBar]) {
    for p in progress {
        p.finish();
    }
}

fn main() -> Result<()> {
    let log_path = env::var("PREFIX_LOG")?;

    let mut logger = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&log_path).join("riselab.log"))?;

    let pb_tmux = riselab::util::new_spinner();
    let pb_minio = riselab::util::new_spinner();
    let pb_compute_node = riselab::util::new_spinner();
    let pb_meta_node = riselab::util::new_spinner();
    let pb_frontend = riselab::util::new_spinner();

    let pbs = vec![
        pb_tmux.clone(),
        pb_minio.clone(),
        pb_compute_node.clone(),
        pb_meta_node.clone(),
        pb_frontend.clone(),
    ];
    let multi_pb = create_multi_progress(&pbs);

    let join_handler = std::thread::spawn(move || multi_pb.join());

    let mut service = ConfigureTmuxTask::new()?;
    pb_tmux.set_prefix("tmux");
    service.execute(&mut logger, pb_tmux)?;

    let mut service = MinioService::new()?;
    pb_minio.set_prefix(service.id());
    service.execute(&mut logger, pb_minio.clone())?;

    let mut task = riselab::ConfigureMinioTask::new()?;
    task.execute(&mut logger, pb_minio)?;

    let mut service = ComputeNodeService::new(5688)?;
    pb_compute_node.set_prefix(service.id());
    service.execute(&mut logger, pb_compute_node.clone())?;

    let mut task = riselab::ConfigureGrpcNodeTask::new(5688)?;
    task.execute(&mut logger, pb_compute_node)?;

    let mut service = MetaNodeService::new(5690)?;
    pb_meta_node.set_prefix(service.id());
    service.execute(&mut logger, pb_meta_node.clone())?;

    let mut task = riselab::ConfigureGrpcNodeTask::new(5690)?;
    task.execute(&mut logger, pb_meta_node)?;

    let mut service = FrontendService::new()?;
    pb_frontend.set_prefix(service.id());
    service.execute(&mut logger, pb_frontend.clone())?;

    let postgres_port = 4567;
    let mut task = riselab::ConfigureGrpcNodeTask::new(postgres_port)?;
    task.execute(&mut logger, pb_frontend)?;

    finish_multi_progress(&pbs);
    join_handler.join().unwrap()?;

    println!("All services started successfully.");

    println!("\nPRO TIPS:");
    println!(
        "Run `tmux a -t {}` to attach to the tmux console.",
        RISELAB_SESSION_NAME
    );
    println!(
        "Run `psql -h localhost -p {} -d dev` to start Postgres interactive shell.",
        postgres_port
    );
    println!("Run `./riselab kill-playground` to kill cluster.");

    Ok(())
}
