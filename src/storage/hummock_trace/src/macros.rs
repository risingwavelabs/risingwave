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

#[macro_export]
macro_rules! trace {
    (GET, $key:ident, $epoch:ident, $opt:ident, $storage_type:expr) => {
        risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::get(
                risingwave_hummock_trace::TracedBytes::from($key.to_vec()),
                $epoch,
                $opt.prefix_hint.clone(),
                $opt.check_bloom_filter,
                $opt.retention_seconds,
                $opt.table_id.table_id,
                $opt.ignore_range_tombstone,
            ),
            $storage_type
        )
    };
    (INGEST, $kvs:ident, $delete_range:ident, $opt:ident, $storage_type:expr) => {
        risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::ingest(
                $kvs.iter()
                    .map(|(k, v)| (
                        risingwave_hummock_trace::TracedBytes::from(k.clone()),
                        v.user_value
                            .clone()
                            .map(|b| risingwave_hummock_trace::TracedBytes::from(b.clone()))
                    ))
                    .collect(),
                $delete_range
                    .iter()
                    .map(|(k, v)| (
                        risingwave_hummock_trace::TracedBytes::from(k.clone()),
                        risingwave_hummock_trace::TracedBytes::from(v.clone())
                    ))
                    .collect(),
                $opt.epoch,
                $opt.table_id.table_id,
            ),
            $storage_type
        );
    };
    (ITER, $range:ident, $epoch:ident, $opt:ident, $storage_type:expr) => {
        risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::Iter {
                key_range: (
                    $range
                        .0
                        .as_ref()
                        .map(|v| risingwave_hummock_trace::TracedBytes::from(v.clone())),
                    $range
                        .1
                        .as_ref()
                        .map(|v| risingwave_hummock_trace::TracedBytes::from(v.clone()))
                ),
                epoch: $epoch,
                read_options: risingwave_hummock_trace::TraceReadOptions {
                    prefix_hint: $opt.prefix_hint.clone(),
                    table_id: $opt.table_id.table_id,
                    retention_seconds: $opt.retention_seconds,
                    check_bloom_filter: $opt.check_bloom_filter,
                    ignore_range_tombstone: $opt.ignore_range_tombstone,
                }
            },
            $storage_type
        );
    };
    (ITER_NEXT, $id:expr, $storage_type:expr) => {
        risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::IterNext($id),
            $storage_type
        );
    };
    (SYNC, $epoch:ident, $storage_type:expr) => {
        risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::Sync($epoch),
            $storage_type
        );
    };
    (SEAL, $epoch:ident, $check_point:ident, $storage_type:expr) => {
        let _span = risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::Seal($epoch, $check_point),
            $storage_type
        );
    };
    (METAMSG, $resp:ident) => {
        let _span = risingwave_hummock_trace::new_global_span!(
            risingwave_hummock_trace::Operation::MetaMessage(Box::new(
                risingwave_hummock_trace::TraceSubResp($resp.clone(),)
            )),
            risingwave_hummock_trace::StorageType::Global
        );
    };
}

#[macro_export]
macro_rules! trace_result {
    (GET, $span:ident, $result:ident) => {
        risingwave_hummock_trace::send_result!(
            $span,
            OperationResult::Get(TraceResult::from($result.as_ref().map(|o| {
                o.as_ref()
                    .map(|b| risingwave_hummock_trace::TracedBytes::from(b.clone()))
            })))
        )
    };
    (INGEST, $span:ident, $result:ident) => {
        risingwave_hummock_trace::send_result!(
            $span,
            OperationResult::Ingest(TraceResult::from($result.as_ref().map(|b| *b)))
        )
    };
    (ITER, $span:ident, $result:ident) => {
        risingwave_hummock_trace::send_result!(
            $span,
            OperationResult::Iter(TraceResult::from($result.as_ref().map(|_| ())))
        )
    };
    (ITER_NEXT, $span:expr, $pair:ident) => {
        risingwave_hummock_trace::send_result!(
            $span,
            OperationResult::IterNext(TraceResult::Ok($pair.as_ref().map(|(k, v)| (
                risingwave_hummock_trace::TracedBytes::from(k.user_key.table_key.to_vec()),
                risingwave_hummock_trace::TracedBytes::from(v.clone())
            ))))
        )
    };
    (SYNC, $span:ident, $result:ident) => {
        risingwave_hummock_trace::send_result!(
            $span,
            OperationResult::Sync(TraceResult::from(
                $result.as_ref().map(|res| res.sync_size.clone())
            ))
        )
    };
}

#[macro_export]
macro_rules! new_global_span {
    ($op:expr, $storage_type:expr) => {
        if risingwave_hummock_trace::should_use_trace() {
            Some(risingwave_hummock_trace::TraceSpan::new_to_global(
                $op,
                $storage_type,
            ))
        } else {
            None
        }
    };
}

#[macro_export]
macro_rules! send_result {
    ($span:expr, $result:expr) => {
        if let Some(span) = &$span {
            span.send_result($result);
        }
    };
}

#[macro_export]
macro_rules! traced_bytes {
    () => (
        $crate::TracedBytes::from(vec![])
    );
    ($elem:expr; $n:expr) => (
        $crate::TracedBytes::from(vec![$elem:expr; $n:expr])
    );
    ($($x:expr),+ $(,)?) => (
        $crate::TracedBytes::from(vec![$($x),+])
    );
}
