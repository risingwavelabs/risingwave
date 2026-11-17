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
use futures_async_stream::try_stream;
use risingwave_connector::source::cdc::external::ExternalTableReaderImpl;
use thiserror_ext::AsReport;
use tracing::Instrument;

use super::external::ExternalStorageTable;
use crate::executor::prelude::{
    BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult,
};
use crate::executor::source::get_infinite_backoff_strategy;
use crate::task::{ActorId, FragmentId};

/// Update shared reader configuration before delivering a connector-change barrier to
/// either backfill executor, including while it is waiting for reader creation.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub(crate) async fn apply_connector_props_changes(
    mut upstream: BoxedMessageStream,
    external_table: ExternalStorageTable,
) {
    while let Some(msg) = upstream.next().await {
        let msg = msg?;
        if let Message::Barrier(barrier) = &msg
            && let Some(mutation) = barrier.mutation.as_deref()
        {
            external_table.update_connector_props(mutation)?;
        }
        yield msg;
    }
}

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
    let mut config = external_table.subscribe_config();
    loop {
        config.borrow_and_update();
        tokio::select! {
            biased;
            _ = config.changed() => {
                // Cancel an in-flight connection attempt or backoff using old credentials.
            }
            reader = retry_create_table_reader(&external_table, actor_id, fragment_id) => {
                return reader;
            }
        }
    }
}

async fn retry_create_table_reader(
    external_table: &ExternalStorageTable,
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

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::{Barrier, Mutation};

    #[tokio::test]
    async fn test_connector_props_change_unblocks_reader_creation() {
        let table = ExternalStorageTable::for_test_undefined(42.into())
            .with_mock_snapshot_errors([0])
            .with_mock_required_password("new_password");
        let mut reader = Box::pin(create_table_reader_with_retry(
            table.clone(),
            1.into(),
            1.into(),
        ));

        // The initial connection fails and enters backoff with the old credentials.
        assert!(reader.as_mut().now_or_never().is_none());
        assert_eq!(table.mock_reader_create_count(), 1);

        // A mutation for another shared source must not wake or reconfigure this reader.
        table
            .update_connector_props(&Mutation::ConnectorPropsChange(
                [(43, Default::default())].into_iter().collect(),
            ))
            .unwrap();
        assert!(reader.as_mut().now_or_never().is_none());
        assert_eq!(table.mock_reader_create_count(), 1);

        let props = [
            ("connector".into(), "mysql-cdc".into()),
            ("hostname".into(), "new_host".into()),
            ("port".into(), "3306".into()),
            ("username".into(), "user".into()),
            ("password".into(), "new_password".into()),
        ]
        .into_iter()
        .collect();
        let mutation = Mutation::ConnectorPropsChange([(42, props)].into_iter().collect());
        let barrier = Barrier::new_test_barrier(test_epoch(2)).with_mutation(mutation);
        let mut upstream = apply_connector_props_changes(
            futures::stream::iter([Ok(Message::Barrier(barrier))]).boxed(),
            table.clone(),
        )
        .boxed();
        assert!(matches!(
            build_reader_and_poll_upstream(&mut upstream, &mut reader)
                .await
                .unwrap(),
            Either::Left(Message::Barrier(_))
        ));

        // Applying the barrier wakes the retry immediately, without waiting out backoff.
        assert!(reader.as_mut().now_or_never().is_some());
        assert_eq!(table.mock_reader_create_count(), 2);
    }
}
