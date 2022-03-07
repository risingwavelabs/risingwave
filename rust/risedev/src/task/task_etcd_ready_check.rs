use anyhow::Result;
use isahc::Body;
use serde::Deserialize;

use crate::{EtcdConfig, ExecuteContext, Task};

#[derive(Deserialize)]
struct HealthResponse {
    health: String,
    #[allow(dead_code)]
    reason: String,
}

pub struct EtcdReadyCheckTask {
    config: EtcdConfig,
}

impl EtcdReadyCheckTask {
    pub fn new(config: EtcdConfig) -> Result<Self> {
        Ok(Self { config })
    }
}

impl Task for EtcdReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.pb.set_message("waiting for online...");
        let health_check_addr =
            format!("http://{}:{}/health", self.config.address, self.config.port);
        let online_cb = |body: Body| -> bool {
            let response: HealthResponse = serde_json::from_reader(body).unwrap();
            response.health == "true"
        };

        ctx.wait_http_with_cb(health_check_addr, online_cb)?;
        ctx.pb
            .set_message(format!("api {}:{}", self.config.address, self.config.port));
        ctx.complete_spin();

        Ok(())
    }
}
