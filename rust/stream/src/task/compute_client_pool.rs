use std::net::SocketAddr;

use moka::future::Cache;
use risingwave_common::error::Result;
use risingwave_rpc_client::ComputeClient;

#[derive(Clone)]
pub struct ComputeClientPool {
    cache: Cache<SocketAddr, ComputeClient>,
}

impl ComputeClientPool {
    pub fn new(cache_capacity: u64) -> Self {
        Self {
            cache: Cache::new(cache_capacity),
        }
    }

    /// Get a compute client from the pool.
    pub async fn get_client_for_addr(&self, addr: &SocketAddr) -> Result<ComputeClient> {
        self.cache
            .get_or_try_insert_with(*addr, async { ComputeClient::new(addr).await })
            .await
            .map_err(|e| {
                // TODO: change this to error when we completed failover and error handling
                panic!("failed to create compute client: {:?}", e)
            })
    }
}
