use anyhow::Result;

use super::{ExecuteContext, Task};

pub struct ConfigureGrpcNodeTask {
    port: u16,
    user_managed: bool,
}

impl ConfigureGrpcNodeTask {
    pub fn new(port: u16, user_managed: bool) -> Result<Self> {
        Ok(Self { port, user_managed })
    }
}

impl Task for ConfigureGrpcNodeTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        let address = format!("127.0.0.1:{}", self.port);

        if self.user_managed {
            ctx.pb
                .set_message("waiting for user-managed service online... (you should start it!)");
            ctx.wait_tcp_user(&address)?;
        } else {
            ctx.pb.set_message("waiting for online...");
            ctx.wait_tcp(&address)?;
        }

        ctx.complete_spin();

        ctx.pb.set_message(format!("api {}", address));

        Ok(())
    }
}
