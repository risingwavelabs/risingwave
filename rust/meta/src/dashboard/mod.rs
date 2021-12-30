use std::net::SocketAddr;

use log::info;
use risingwave_common::error::Result;
use warp::Filter;

pub struct DashboardService {
    addr: SocketAddr,
}

impl DashboardService {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn serve(self) -> Result<()> {
        info!("starting dashboard service at {:?}", self.addr);
        let hello = warp::path!("hello" / String).map(|name| format!("Hello, {}!", name));
        warp::serve(hello).run(self.addr).await;
        Ok(())
    }
}
