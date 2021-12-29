use anyhow::Result;

use super::{ExecuteContext, Task};

pub struct ConfigureGrpcNodeTask {
    port: u16,
}

impl ConfigureGrpcNodeTask {
    pub fn new(port: u16) -> Result<Self> {
        Ok(Self { port })
    }
}

impl Task for ConfigureGrpcNodeTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.pb.set_message("waiting for online...");
        let compute_node_address = format!("127.0.0.1:{}", self.port);
        ctx.wait_tcp(&compute_node_address)?;

        ctx.complete_spin();

        ctx.pb.set_message(format!("api {}", compute_node_address));

        Ok(())
    }
}
