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

use std::fmt::{Debug, Formatter};
use std::future::Future;

use futures::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::{self, Plan};
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::task_service::{ExecuteRequest, GetDataResponse};
use risingwave_rpc_client::ComputeClient;
use tonic::Streaming;

use crate::exchange_source::ExchangeSource;
use crate::task::TaskId;

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    stream: Streaming<GetDataResponse>,

    task_output_id: TaskOutputId,
}

impl GrpcExchangeSource {
    pub async fn create(
        client: ComputeClient,
        task_output_id: TaskOutputId,
        local_execute_plan: Option<LocalExecutePlan>,
    ) -> Result<Self> {
        let task_id = task_output_id.get_task_id()?.clone();
        let stream = match local_execute_plan {
            // When in the local execution mode, `GrpcExchangeSource` would send out
            // `ExecuteRequest` and get the data chunks back in a single RPC.
            Some(local_execute_plan) => {
                let plan = try_match_expand!(local_execute_plan, Plan)?;
                let execute_request = ExecuteRequest {
                    task_id: Some(task_id),
                    plan: plan.plan,
                    epoch: plan.epoch,
                };
                println!("local execute RPC for task_output_id: {:?}", task_output_id);
                let a = client.execute(execute_request).await;
                if let Err(e) = a.as_ref() {
                    println!("error for {:?}", e);
                } else {

                }
                println!("local execute RPC for task_output_id Done: {:?}", task_output_id);
                a.unwrap()
            }
            None => {
                let a = task_output_id.task_id.as_ref().unwrap();
                println!(
                    "---------get by exchange rpc done for task_id {:?}, stage_id {:?}, query_id {:?} start",
                    a.task_id, a.stage_id, a.query_id
                );
                let b = client.get_data(task_output_id.clone()).await.unwrap();
                println!(
                    "---------get by exchange rpc done for task_id {:?}, stage_id {:?}, query_id {:?} done",
                    a.task_id, a.stage_id, a.query_id
                );
                b
            }
        };
        let source = Self {
            stream,
            task_output_id,
        };
        Ok(source)
    }
}

impl Debug for GrpcExchangeSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcExchangeSource")
            .field("task_output_id", &self.task_output_id)
            .finish()
    }
}

impl ExchangeSource for GrpcExchangeSource {
    type TakeDataFuture<'a> = impl Future<Output = Result<Option<DataChunk>>>;

    fn take_data(&mut self) -> Self::TakeDataFuture<'_> {
        async {
            let res = match self.stream.next().await {
                None => return Ok(None),
                Some(r) => r,
            };
            let task_data = res?;
            let data = DataChunk::from_protobuf(task_data.get_record_batch()?)?.compact()?;
            trace!(
                "Receiver taskOutput = {:?}, data = {:?}",
                self.task_output_id,
                data
            );

            Ok(Some(data))
        }
    }

    fn get_task_id(&self) -> TaskId {
        TaskId::from(self.task_output_id.get_task_id().unwrap())
    }
}
