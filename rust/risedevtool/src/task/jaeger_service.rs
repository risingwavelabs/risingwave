use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};
use crate::JaegerConfig;

pub struct JaegerService {
    config: JaegerConfig,
}

impl JaegerService {
    pub fn new(config: JaegerConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn jaeger(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(
            Path::new(&prefix_bin)
                .join("jaeger")
                .join("jaeger-all-in-one"),
        ))
    }
}

impl Task for JaegerService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.jaeger()?;
        cmd.arg("--admin.http.host-port")
            .arg("127.0.0.1:14269")
            .arg("--collector.grpc-server.host-port")
            .arg("127.0.0.1:14250")
            .arg("--collector.http-server.host-port")
            .arg("127.0.0.1:14268")
            .arg("--collector.queue-size")
            .arg("65536")
            .arg("--http-server.host-port")
            .arg("127.0.0.1:5778")
            .arg("--processor.jaeger-binary.server-host-port")
            .arg("127.0.0.1:6832")
            .arg("--processor.jaeger-compact.server-host-port")
            .arg("127.0.0.1:6831")
            .arg("--processor.zipkin-compact.server-host-port")
            .arg("127.0.0.1:5775")
            .arg("--query.grpc-server.host-port")
            .arg("127.0.0.1:16685")
            .arg("--query.http-server.host-port")
            .arg(format!(
                "{}:{}",
                self.config.dashboard_address, self.config.dashboard_port
            ));

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
