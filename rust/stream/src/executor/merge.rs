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

use futures::channel::mpsc::Sender;
use futures::{SinkExt, StreamExt};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::task_service::GetStreamResponse;
use risingwave_rpc_client::ComputeClient;
use risingwave_storage::StateStore;
use tonic::Streaming;

use super::{Executor, Message, Result};
use crate::executor::ExecutorBuilder;
use crate::task::{ExecutorParams, LocalStreamManagerCore, UpDownActorIds};

/// Receive data from `gRPC` and forwards to `MergerExecutor`/`ReceiverExecutor`
pub struct RemoteInput {
    stream: Streaming<GetStreamResponse>,
    sender: Sender<Message>,
}

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn create(
        client: ComputeClient,
        up_down_ids: UpDownActorIds,
        sender: Sender<Message>,
    ) -> Result<Self> {
        let stream = client.get_stream(up_down_ids.0, up_down_ids.1).await?;
        Ok(Self { stream, sender })
    }

    pub async fn run(&mut self) {
        loop {
            let data = self.stream.next().await;
            match data {
                // the connection from rpc server is closed, then break the loop
                None => break,
                Some(data_res) => match data_res {
                    Ok(stream_msg) => {
                        let msg_res = Message::from_protobuf(
                            stream_msg
                                .get_message()
                                .expect("no message in stream response!"),
                        );
                        match msg_res {
                            Ok(msg) => {
                                let _ = self.sender.send(msg).await;
                            }
                            Err(e) => {
                                info!("RemoteInput forward message error:{}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        info!("RemoteInput tonic error status:{}", e);
                        break;
                    }
                },
            }
        }
    }
}

pub struct MergeExecutorBuilder {}

impl ExecutorBuilder for MergeExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::MergeNode)?;
        stream.create_merge_node(params, node)
    }
}
