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

use std::collections::{BTreeMap, VecDeque};
use std::future::{pending, ready};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use futures::channel::oneshot;
use futures::poll;
use lapin::message::{BasicReturnMessage, Delivery};
use risingwave_common::array::StreamChunkTestExt;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use serde_json::json;
use url::Url;

use super::*;
use crate::sink::catalog::{SinkEncode, SinkFormat, SinkFormatDesc};
use crate::sink::encoder::{
    DateHandlingMode, JsonEncoder, JsonbHandlingMode, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::log_store::{DeliveryFutureManager, TruncateOffset};

fn format_desc(encode: SinkEncode) -> SinkFormatDesc {
    SinkFormatDesc {
        format: SinkFormat::AppendOnly,
        encode,
        options: BTreeMap::new(),
        secret_refs: BTreeMap::new(),
        key_encode: None,
        connection_id: None,
    }
}

fn protobuf_format() -> SinkFormatDesc {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("codec/tests/test_data/all-types.pb");
    let mut format = format_desc(SinkEncode::Protobuf);
    format.options = BTreeMap::from([
        (
            "schema.location".into(),
            Url::from_file_path(path).unwrap().to_string(),
        ),
        ("message".into(), "all_types.AllTypes.NestedMessage".into()),
    ]);
    format
}

fn config() -> RabbitMqConfig {
    RabbitMqConfig::from_btreemap(BTreeMap::from([
        ("url".into(), "amqp://localhost".into()),
        ("username".into(), "guest".into()),
        ("password".into(), "guest".into()),
        ("exchange".into(), "".into()),
        ("routing_key".into(), "test".into()),
        ("type".into(), "append-only".into()),
    ]))
    .unwrap()
}

fn encoder() -> RabbitMqEncoder {
    JsonEncoder::new(
        Schema::new(vec![
            Field::with_name(DataType::Int32, "id"),
            Field::with_name(DataType::Varchar, "value"),
        ]),
        None,
        DateHandlingMode::FromCe,
        TimestampHandlingMode::Milli,
        TimestamptzHandlingMode::UtcWithoutSuffix,
        TimeHandlingMode::Milli,
        JsonbHandlingMode::String,
    )
    .into()
}

fn chunk() -> StreamChunk {
    StreamChunk::from_pretty("i T\n + 1 hello\n + 2 .")
}

fn returned_message() -> BasicReturnMessage {
    BasicReturnMessage {
        delivery: Delivery::mock(
            0,
            "events".into(),
            "missing".into(),
            false,
            b"private message body".to_vec(),
        ),
        reply_code: 312,
        reply_text: "NO_ROUTE".into(),
    }
}

fn connection_error() -> lapin::Error {
    std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset").into()
}

#[tokio::test]
async fn classifies_confirmations_without_exposing_payload() {
    let deadline = Instant::now() + Duration::from_secs(5);
    delivery_future(ready(Ok(Confirmation::Ack(None))), deadline)
        .await
        .unwrap();

    for (confirmation, expected) in [
        (Confirmation::Ack(Some(returned_message())), "312"),
        (Confirmation::Nack(None), "negatively acknowledged"),
        (
            Confirmation::Nack(Some(returned_message())),
            "negatively acknowledged",
        ),
        (Confirmation::NotRequested, "not enabled"),
    ] {
        let error = delivery_future(ready(Ok(confirmation)), deadline)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("private message body"));
    }

    let error = delivery_future(ready(Err(connection_error())), deadline)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("failed to confirm"));
}

#[tokio::test]
async fn encodes_visible_rows_as_persistent_mandatory_json() {
    let mut manager = DeliveryFutureManager::new(8);
    let mut messages = vec![];
    publish_chunk(
        &config(),
        &encoder(),
        StreamChunk::from_pretty("i T\n + 1 hello\n + 99 hidden D\n + 2 ."),
        manager.start_write_chunk(1, 0),
        |payload, options, properties| {
            assert!(options.mandatory);
            assert!(!options.immediate);
            assert_eq!(properties.delivery_mode(), &Some(2));
            assert_eq!(
                properties.content_type().as_ref().unwrap().as_str(),
                "application/json"
            );
            messages.push(serde_json::from_slice::<serde_json::Value>(&payload).unwrap());
            ready(Ok(ready(Ok(Confirmation::Ack(None)))))
        },
    )
    .await
    .unwrap();
    assert_eq!(
        messages,
        vec![
            json!({"id": 1, "value": "hello"}),
            json!({"id": 2, "value": null})
        ]
    );
    assert_eq!(
        manager.next_truncate_offset().await.unwrap(),
        TruncateOffset::Chunk {
            epoch: 1,
            chunk_id: 0
        }
    );
}

#[tokio::test]
async fn publishes_protobuf_loaded_from_descriptor() {
    let schema = Schema::new(vec![
        Field::with_name(DataType::Int32, "id"),
        Field::with_name(DataType::Varchar, "name"),
    ]);
    let encoder = RabbitMqEncoder::new(schema, &protobuf_format(), "test")
        .await
        .unwrap();
    let mut manager = DeliveryFutureManager::new(8);
    let mut messages = vec![];
    publish_chunk(
        &config(),
        &encoder,
        chunk(),
        manager.start_write_chunk(1, 0),
        |payload, options, properties| {
            assert!(options.mandatory);
            assert_eq!(properties.delivery_mode(), &Some(2));
            assert_eq!(
                properties.content_type().as_ref().unwrap().as_str(),
                "application/x-protobuf"
            );
            messages.push(payload);
            ready(Ok(ready(Ok(Confirmation::Ack(None)))))
        },
    )
    .await
    .unwrap();
    // A local descriptor uses raw Protobuf, without a schema registry header.
    assert_eq!(
        messages,
        vec![b"\x08\x01\x12\x05hello".to_vec(), vec![8, 2]]
    );
    manager.next_truncate_offset().await.unwrap();
}

#[tokio::test]
async fn validates_encoding_before_connecting() {
    let schema = Schema::new(vec![Field::with_name(DataType::Int32, "id")]);
    let mut upsert = format_desc(SinkEncode::Json);
    upsert.format = SinkFormat::Upsert;
    let mut key_encode = format_desc(SinkEncode::Json);
    key_encode.key_encode = Some(SinkEncode::Json);
    let mut missing_schema = protobuf_format();
    missing_schema.options.remove("schema.location");
    for (format, expected) in [
        (upsert, "append-only"),
        (key_encode, "KEY ENCODE"),
        (format_desc(SinkEncode::Avro), "unsupported"),
        (format_desc(SinkEncode::Bytes), "unsupported"),
        (format_desc(SinkEncode::Protobuf), "message"),
        (missing_schema, "schema.location"),
    ] {
        let error = RabbitMqEncoder::new(schema.clone(), &format, "test")
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }

    let wrong_type = Schema::new(vec![Field::with_name(DataType::Varchar, "id")]);
    assert!(
        RabbitMqEncoder::new(wrong_type, &protobuf_format(), "test")
            .await
            .is_err()
    );
    let json = RabbitMqEncoder::new(schema, &format_desc(SinkEncode::Json), "test")
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &json.encode(OwnedRow::new(vec![Some(1.into())])).unwrap()
        )
        .unwrap(),
        json!({"id": 1})
    );
}

#[tokio::test]
async fn rejects_changes_before_publishing_any_row() {
    for input in [
        "i T\n + 1 hello\n - 2 old",
        "i T\n + 1 hello\n U- 2 old\n U+ 2 new",
    ] {
        let mut manager = DeliveryFutureManager::new(8);
        let mut submitted = 0;
        let error = publish_chunk(
            &config(),
            &encoder(),
            StreamChunk::from_pretty(input),
            manager.start_write_chunk(1, 0),
            |_, _, _| {
                submitted += 1;
                ready(Ok(ready(Ok(Confirmation::Ack(None)))))
            },
        )
        .await
        .unwrap_err();
        assert_eq!(submitted, 0);
        assert!(error.to_string().contains("only supports insert"));
    }
}

#[tokio::test]
async fn enforces_encoded_byte_size_limit() {
    let input = StreamChunk::from_pretty("i T\n + 1 中文");
    let size = serde_json::to_vec(&json!({"id": 1, "value": "中文"}))
        .unwrap()
        .len();
    for limit in [size - 1, size] {
        let mut config = config();
        config.max_message_size = limit;
        let mut manager = DeliveryFutureManager::new(8);
        let mut published = false;
        let result = publish_chunk(
            &config,
            &encoder(),
            input.clone(),
            manager.start_write_chunk(1, 0),
            |_, _, _| {
                published = true;
                ready(Ok(ready(Ok(Confirmation::Ack(None)))))
            },
        )
        .await;
        assert_eq!(published, limit == size);
        if limit == size {
            result.unwrap();
        } else {
            assert!(result.unwrap_err().to_string().contains("max_message_size"));
        }
    }
}

#[tokio::test]
async fn reserves_capacity_before_submitting_next_publish() {
    // Honor both the writer's configured window and the log sinker's window.
    for (writer_limit, manager_limit) in [(1, 8), (8, 1)] {
        let mut config = config();
        config.max_inflight_messages = writer_limit;
        let encoder = encoder();
        let mut manager = DeliveryFutureManager::new(manager_limit);
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let mut confirms = VecDeque::from([first_rx, second_rx]);
        let submitted = AtomicUsize::new(0);
        {
            let mut write = pin!(publish_chunk(
                &config,
                &encoder,
                chunk(),
                manager.start_write_chunk(1, 0),
                |_, _, _| {
                    submitted.fetch_add(1, Ordering::Relaxed);
                    let rx = confirms.pop_front().unwrap();
                    ready(Ok(async move { rx.await.unwrap() }))
                },
            ));
            assert!(poll!(&mut write).is_pending());
            assert_eq!(submitted.load(Ordering::Relaxed), 1);
            first_tx.send(Ok(Confirmation::Ack(None))).unwrap();
            write.await.unwrap();
            assert_eq!(submitted.load(Ordering::Relaxed), 2);
        }
        assert!(manager.next_truncate_offset().now_or_never().is_none());
        second_tx.send(Ok(Confirmation::Ack(None))).unwrap();
        assert_eq!(
            manager.next_truncate_offset().await.unwrap(),
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 0
            }
        );
    }
}

#[tokio::test]
async fn failed_confirmation_stops_backpressured_publishing() {
    let mut config = config();
    config.max_inflight_messages = 1;
    let mut manager = DeliveryFutureManager::new(1);
    let mut submitted = 0;
    let error = publish_chunk(
        &config,
        &encoder(),
        chunk(),
        manager.start_write_chunk(1, 0),
        |_, _, _| {
            submitted += 1;
            ready(Ok(ready(Ok(Confirmation::Nack(None)))))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(submitted, 1);
    assert!(error.to_string().contains("negatively acknowledged"));
}

#[tokio::test]
async fn out_of_order_confirms_do_not_advance_past_pending_chunk() {
    let mut manager = DeliveryFutureManager::new(8);
    let (first_tx, first_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();
    for (chunk_id, rx) in [first_rx, second_rx].into_iter().enumerate() {
        let mut rx = Some(rx);
        publish_chunk(
            &config(),
            &encoder(),
            StreamChunk::from_pretty("i T\n + 1 hello"),
            manager.start_write_chunk(1, chunk_id as _),
            |_, _, _| {
                let rx = rx.take().unwrap();
                ready(Ok(async move { rx.await.unwrap() }))
            },
        )
        .await
        .unwrap();
    }
    manager.add_barrier(1);
    second_tx.send(Ok(Confirmation::Ack(None))).unwrap();
    assert!(manager.next_truncate_offset().now_or_never().is_none());
    first_tx.send(Ok(Confirmation::Ack(None))).unwrap();
    assert_eq!(
        manager.next_truncate_offset().await.unwrap(),
        TruncateOffset::Barrier { epoch: 1 }
    );
}

#[tokio::test]
async fn returned_message_blocks_log_truncation() {
    let mut manager = DeliveryFutureManager::new(8);
    publish_chunk(
        &config(),
        &encoder(),
        chunk(),
        manager.start_write_chunk(1, 0),
        |_, _, _| ready(Ok(ready(Ok(Confirmation::Ack(Some(returned_message())))))),
    )
    .await
    .unwrap();
    manager.add_barrier(1);
    assert!(
        manager
            .next_truncate_offset()
            .await
            .unwrap_err()
            .to_string()
            .contains("NO_ROUTE")
    );
}

#[tokio::test]
async fn confirm_timeout_includes_time_queued_before_first_poll() {
    let deadline = Instant::now() + Duration::from_millis(10);
    let future = delivery_future(pending(), deadline);
    // Simulate waiting behind an earlier message without polling this future.
    tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
    let mut future = pin!(future);
    let Poll::Ready(Err(error)) = poll!(&mut future) else {
        panic!("expired confirmation did not fail on its first poll");
    };
    assert!(error.to_string().contains("confirm timed out"));
}

#[tokio::test]
async fn publish_failure_and_timeout_stop_the_chunk() {
    let mut config = config();
    config.publish_timeout_ms = 10;
    for stalled in [false, true] {
        let mut manager = DeliveryFutureManager::new(8);
        let mut submitted = 0;
        let result =
            timeout(
                Duration::from_secs(5),
                publish_chunk(
                    &config,
                    &encoder(),
                    chunk(),
                    manager.start_write_chunk(1, 0),
                    |_, _, _| {
                        submitted += 1;
                        async move {
                            if stalled {
                                pending::<()>().await;
                            }
                            Err::<std::future::Ready<lapin::Result<Confirmation>>, _>(
                                connection_error(),
                            )
                        }
                    },
                ),
            )
            .await
            .expect("publish timeout was not enforced");
        assert_eq!(submitted, 1);
        let error = result.unwrap_err().to_string();
        assert!(error.contains(if stalled {
            "publish submission timed out"
        } else {
            "failed to publish"
        }));
    }
}
