use crate::meta::MetaManager;
use async_trait::async_trait;
use risingwave_common::error::Result;
use risingwave_pb::stream_service::ActorInfo;

#[async_trait]
pub trait StreamMetaManager {
    async fn fetch_actor_info(&self) -> Result<Vec<ActorInfo>>;
    async fn add_fragment_to_worker(&self) -> Result<()>;
}

#[async_trait]
impl StreamMetaManager for MetaManager {
    async fn fetch_actor_info(&self) -> Result<Vec<ActorInfo>> {
        todo!();
    }

    async fn add_fragment_to_worker(&self) -> Result<()> {
        todo!();
    }
}
