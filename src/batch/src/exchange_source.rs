use std::fmt::Debug;
use std::future::Future;

use risingwave_common::array::DataChunk;

use crate::execution::grpc_exchange::GrpcExchangeSource;
use crate::execution::local_exchange::LocalExchangeSource;
use crate::executor::test_utils::FakeExchangeSource;

/// Each `ExchangeSource` maps to one task, it takes the execution result from task chunk by chunk.
pub trait ExchangeSource: Send + Debug {
    type TakeDataFuture<'a>: Future<Output = risingwave_common::error::Result<Option<DataChunk>>>
        + 'a
    where
        Self: 'a;
    fn take_data(&mut self) -> Self::TakeDataFuture<'_>;
}

#[derive(Debug)]
pub enum ExchangeSourceImpl {
    Grpc(GrpcExchangeSource),
    Local(LocalExchangeSource),
    Fake(FakeExchangeSource),
}

impl ExchangeSourceImpl {
    pub(crate) async fn take_data(
        &mut self,
    ) -> risingwave_common::error::Result<Option<DataChunk>> {
        match self {
            ExchangeSourceImpl::Grpc(grpc) => grpc.take_data().await,
            ExchangeSourceImpl::Local(local) => local.take_data().await,
            ExchangeSourceImpl::Fake(fake) => fake.take_data().await,
        }
    }
}
