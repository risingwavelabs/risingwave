// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::{Stream, StreamExt, TryStreamExt};
use risingwave_common::catalog::TableDesc;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{ScalarRef, ScalarRefImpl};
use risingwave_connector::source::cdc::Oracle;
use risingwave_connector::source::cdc::split::DebeziumCdcSplit;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::*;
use risingwave_storage::dispatch_state_store;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct StreamServiceImpl {
    pub mgr: LocalStreamManager,
    pub env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: LocalStreamManager, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    type StreamingControlStreamStream =
        impl Stream<Item = std::result::Result<StreamingControlStreamResponse, tonic::Status>>;

    async fn streaming_control_stream(
        &self,
        request: Request<Streaming<StreamingControlStreamRequest>>,
    ) -> Result<Response<Self::StreamingControlStreamStream>, Status> {
        let mut stream = request.into_inner().boxed();
        let first_request = stream.try_next().await?;
        let Some(StreamingControlStreamRequest {
            request: Some(streaming_control_stream_request::Request::Init(init_request)),
        }) = first_request
        else {
            return Err(Status::invalid_argument(format!(
                "unexpected first request: {:?}",
                first_request
            )));
        };
        let (tx, rx) = unbounded_channel();
        self.mgr.handle_new_control_stream(tx, stream, init_request);
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn get_oracle_initial_mining_scn(
        &self,
        request: Request<GetOracleInitialMiningScnRequest>,
    ) -> Result<Response<GetOracleInitialMiningScnResponse>, Status> {
        let request = request.into_inner();
        let table = request
            .source_state_table
            .ok_or_else(|| Status::invalid_argument("missing source state table"))?;
        let table_desc = TableDesc::from_pb_table(&table);
        let column_ids = table_desc
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect();
        let table_desc = table_desc
            .try_to_protobuf()
            .map_err(|err| Status::internal(err.to_string()))?;
        let initial_mining_scn = dispatch_state_store!(self.env.state_store(), store, {
            // Read the committed snapshot without registering a streaming writer or epoch.
            let reader = BatchTable::new_partial(store, column_ids, None, &table_desc);
            let split = SplitImpl::OracleCdc(DebeziumCdcSplit::<Oracle>::new(
                request.source_id,
                None,
                None,
            ));
            let row = reader
                .get_row(
                    OwnedRow::new(vec![Some(split.id().to_string().into())]),
                    HummockReadEpoch::Committed(request.committed_epoch),
                )
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
            let recovered = match row.as_ref().map(|row| row.datum_at(1)) {
                Some(Some(ScalarRefImpl::Jsonb(value))) => Some(
                    SplitImpl::restore_from_json(value.to_owned_scalar())
                        .map_err(|err| Status::internal(err.to_string()))?,
                ),
                _ => None,
            };
            let Some(SplitImpl::OracleCdc(split)) = recovered else {
                return Err(Status::failed_precondition(
                    "Oracle CDC source split is not checkpointed",
                ));
            };
            split
                .oracle_split
                .and_then(|oracle| oracle.initial_mining_scn)
                .ok_or_else(|| {
                    Status::failed_precondition("Oracle CDC mining SCN is not checkpointed")
                })?
        });
        Ok(Response::new(GetOracleInitialMiningScnResponse {
            initial_mining_scn,
        }))
    }

    async fn get_min_uncommitted_object_id(
        &self,
        _request: Request<GetMinUncommittedObjectIdRequest>,
    ) -> Result<Response<GetMinUncommittedObjectIdResponse>, Status> {
        let min_uncommitted_object_id =
            if let Some(hummock) = self.mgr.env.state_store().as_hummock() {
                hummock.min_uncommitted_object_id().await
            } else {
                None
            }
            .unwrap_or_else(|| u64::MAX.into());
        Ok(Response::new(GetMinUncommittedObjectIdResponse {
            min_uncommitted_object_id,
        }))
    }
}
