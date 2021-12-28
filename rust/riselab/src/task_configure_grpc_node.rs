use anyhow::Result;
use indicatif::ProgressBar;

use crate::util::pb_success;
use crate::wait_tcp::wait_tcp;

pub struct ConfigureGrpcNodeTask {
    port: u16,
}

impl ConfigureGrpcNodeTask {
    pub fn new(port: u16) -> Result<Self> {
        Ok(Self { port })
    }

    pub fn execute(&mut self, f: &mut impl std::io::Write, pb: ProgressBar) -> Result<()> {
        pb.set_message("waiting for online...");
        let compute_node_address = format!("127.0.0.1:{}", self.port);
        wait_tcp(&compute_node_address, f)?;

        pb_success(&pb);

        pb.set_message(format!("api {}", compute_node_address));

        Ok(())
    }
}
