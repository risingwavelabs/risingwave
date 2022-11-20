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
        $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::get(
                $key.to_vec(),
                $epoch,
                $opt.prefix_hint.clone(),
                $opt.check_bloom_filter,
                $opt.retention_seconds,
                $opt.table_id.table_id,
            ),
            $storage_type,
        );
    };
    (INGEST, $kvs:ident, $delete_range:ident, $opt:ident, $storage_type:expr) => {
        $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::ingest(
                $kvs.iter()
                    .map(|(k, v)| (k.to_vec(), v.user_value.clone().map(|b| b.to_vec())))
                    .collect(),
                $delete_range
                    .iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect(),
                $opt.epoch,
                $opt.table_id.table_id,
            ),
            $storage_type,
        );
    };
    (ITER, $range:ident, $epoch:ident, $opt:ident, $storage_type:expr) => {
        $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::Iter {
                prefix_hint: $opt.prefix_hint.clone(),
                key_range: $range.clone(),
                epoch: $epoch,
                table_id: $opt.table_id.table_id,
                retention_seconds: $opt.retention_seconds,
                check_bloom_filter: $opt.check_bloom_filter,
            },
            $storage_type,
        );
    };
    (ITER_NEXT, $id:expr, $storage_type:expr) => {
        $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::IterNext($id),
            $storage_type,
        );
    };
    (SYNC, $epoch:ident, $storage_type:expr) => {
        $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::Sync($epoch),
            $storage_type,
        );
    };
    (SEAL, $epoch:ident, $check_point:ident, $storage_type:expr) => {
        let _span = $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::Seal($epoch, $check_point),
            $storage_type,
        );
    };
    (VERSION) => {
        let _span = $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::UpdateVersion(),
            risingwave_common::hm_trace::TraceLocalId::None,
        );
    };
    (METAMSG, $resp:ident) => {
        let _span = $crate::collector::TraceSpan::new_to_global(
            $crate::record::Operation::MetaMessage(Box::new($crate::record::TraceSubResp(
                $resp.clone(),
            ))),
            $crate::collector::StorageType::Global,
        );
    };
}

#[macro_export]
macro_rules! trace_result {
    (GET, $span:ident, $result:ident) => {
        let res: TraceResult<Option<Vec<u8>>> =
            TraceResult::from($result.as_ref().map(|o| o.as_ref().map(|b| b.to_vec())));
        $span.send_result(OperationResult::Get(res));
    };
    (INGEST, $span:ident, $result:ident) => {
        let res = TraceResult::from($result.as_ref().map(|b| *b));
        $span.send_result(OperationResult::Ingest(res));
    };
    (ITER, $span:ident, $result:ident) => {
        let res = TraceResult::from($result.as_ref().map(|_| ()));
        $span.send_result(OperationResult::Iter(res));
    };
    (ITER_NEXT, $span:expr, $pair:ident) => {
        let res = $pair
            .as_ref()
            .map(|(k, v)| (k.user_key.table_key.to_vec(), v.to_vec()));
        $span.send_result(OperationResult::IterNext(TraceResult::Ok(res)));
    };
    (SYNC, $span:ident, $result:ident) => {
        let res = TraceResult::from($result.as_ref().map(|res| res.sync_size.clone()));
        $span.send_result(OperationResult::Sync(res));
    };
}
