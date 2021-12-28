use std::env;
use std::process::Command;

use anyhow::Result;
use indicatif::ProgressBar;

use crate::util::{pb_success, run_command};
use crate::wait_tcp::wait_tcp;

const HUMMOCK_REMOTE_NAME: &str = "hummock-minio";

pub struct ConfigureMinioTask {
    mcli_path: String,
    mcli_config_path: String,
}

impl ConfigureMinioTask {
    pub fn new() -> Result<Self> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        let prefix_config = env::var("PREFIX_CONFIG")?;
        Ok(Self {
            mcli_path: format!("{}/mcli", prefix_bin),
            mcli_config_path: format!("{}/mcli", prefix_config),
        })
    }

    fn mcli(&mut self) -> Command {
        let mut cmd = Command::new(self.mcli_path.clone());
        cmd.arg("-C").arg(&self.mcli_config_path);
        cmd
    }

    pub fn execute(&mut self, f: &mut impl std::io::Write, pb: ProgressBar) -> Result<()> {
        pb.set_message("waiting for online...");
        let minio_address = env::var("HUMOOCK_MINIO_ADDRESS")?;
        let minio_console_address = env::var("HUMOOCK_MINIO_CONSOLE_ADDRESS")?;
        wait_tcp(&minio_address, f)?;
        wait_tcp(&minio_console_address, f)?;

        pb.set_message("configure...");

        let mut cmd = self.mcli();
        cmd.arg("alias")
            .arg("set")
            .arg(HUMMOCK_REMOTE_NAME)
            .arg(format!("http://{}", env::var("HUMOOCK_MINIO_ADDRESS")?))
            .arg(env::var("MINIO_ROOT_USER")?)
            .arg(env::var("MINIO_ROOT_PASSWORD")?);

        run_command(cmd, f)?;

        let mut cmd = self.mcli();
        cmd.arg("admin")
            .arg("user")
            .arg("add")
            .arg(format!("{}/", HUMMOCK_REMOTE_NAME))
            .arg(env::var("MINIO_HUMMOCK_USER")?)
            .arg(env::var("MINIO_HUMMOCK_PASSWORD")?);
        run_command(cmd, f)?;

        let mut cmd = self.mcli();
        cmd.arg("admin")
            .arg("policy")
            .arg("set")
            .arg(format!("{}/", HUMMOCK_REMOTE_NAME))
            .arg("readwrite")
            .arg(format!("user={}", env::var("MINIO_HUMMOCK_USER")?));
        run_command(cmd, f)?;

        let mut cmd = self.mcli();
        cmd.arg("mb").arg(format!(
            "{}/{}",
            HUMMOCK_REMOTE_NAME,
            env::var("MINIO_BUCKET_NAME")?
        ));
        run_command(cmd, f).ok();

        pb_success(&pb);

        pb.set_message(format!(
            "api {}, console {}",
            minio_address, minio_console_address
        ));

        Ok(())
    }
}
