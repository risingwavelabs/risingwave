// Copyright 2026 RisingWave Labs
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

use std::future::Future;
use std::pin::Pin;

use either::Either;
use futures::{Stream, StreamExt};
use risingwave_connector::source::cdc::external::ExternalTableReaderImpl;
use thiserror_ext::AsReport;
use tracing::Instrument;

use super::external::ExternalStorageTable;
use crate::executor::prelude::{Message, StreamExecutorResult};
use crate::executor::source::get_infinite_backoff_strategy;
use crate::task::{ActorId, FragmentId};

pub(crate) async fn build_reader_and_poll_upstream(
    upstream: &mut (impl Stream<Item = StreamExecutorResult<Message>> + Unpin),
    future: &mut Pin<Box<impl Future<Output = ExternalTableReaderImpl>>>,
) -> StreamExecutorResult<Either<Message, ExternalTableReaderImpl>> {
    tokio::select! {
        biased;
        reader = &mut *future => Ok(Either::Right(reader)),
        msg = upstream.next() => {
            msg.transpose()?
                .map(Either::Left)
                .ok_or_else(|| anyhow::anyhow!(
                    "upstream closed while creating CDC table reader"
                ).into())
        }
    }
}

pub(crate) async fn create_table_reader_with_retry(
    external_table: ExternalStorageTable,
    actor_id: ActorId,
    fragment_id: FragmentId,
) -> ExternalTableReaderImpl {
    let backoff = get_infinite_backoff_strategy();

    tokio_retry::Retry::spawn(backoff, || async {
        match external_table.create_table_reader().await {
            Ok(reader) => Ok(reader),
            Err(error) => {
                tracing::warn!(
                    error = %error.as_report(),
                    actor_id = %actor_id,
                    fragment_id = %fragment_id,
                    "failed to create CDC table reader; retrying"
                );
                Err(error)
            }
        }
    })
    .instrument(tracing::info_span!("create_cdc_table_reader_with_retry"))
    .await
    .expect("retry creating CDC table reader until success")
}
