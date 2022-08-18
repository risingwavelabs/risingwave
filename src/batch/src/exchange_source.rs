// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::future::Future;

use risingwave_common::array::DataChunk;

use crate::execution::grpc_exchange::GrpcExchangeSource;
use crate::execution::local_exchange::LocalExchangeSource;
use crate::executor::test_utils::FakeExchangeSource;
use crate::task::TaskId;

/// Each `ExchangeSource` maps to one task, it takes the execution result from task chunk by chunk.
pub trait ExchangeSource: Send + Debug {
    type TakeDataFuture<'a>: Future<Output = risingwave_common::error::Result<Option<DataChunk>>>
        + 'a
    where
        Self: 'a;
    fn take_data(&mut self) -> Self::TakeDataFuture<'_>;

    /// Get upstream task id.
    fn get_task_id(&self) -> TaskId;
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

    pub(crate) fn get_task_id(&self) -> TaskId {
        match self {
            ExchangeSourceImpl::Grpc(grpc) => grpc.get_task_id(),
            ExchangeSourceImpl::Local(local) => local.get_task_id(),
            ExchangeSourceImpl::Fake(fake) => fake.get_task_id(),
        }
    }
}
