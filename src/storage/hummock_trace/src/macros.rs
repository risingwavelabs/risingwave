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
    (GET, $key:ident, $bloom_filter:ident, $opt:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::Get(
                $key.to_vec(),
                $bloom_filter,
                $opt.epoch,
                $opt.table_id.table_id,
                $opt.retention_seconds,
            ),
            risingwave_common::hm_trace::task_local_get(),
        )
    };
    (INGEST, $kvs:ident, $opt:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::Ingest(
                $kvs.iter()
                    .map(|(k, v)| (k.to_vec(), v.user_value.clone().map(|v| v.to_vec())))
                    .collect(),
                $opt.epoch,
                $opt.table_id.table_id,
            ),
            risingwave_common::hm_trace::task_local_get(),
        )
    };
    (ITER, $prefix:ident, $range:ident, $opt:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::Iter(
                $prefix.clone(),
                $range.0.clone(),
                $range.1.clone(),
                $opt.epoch,
                $opt.table_id.table_id,
                $opt.retention_seconds,
            ),
            risingwave_common::hm_trace::task_local_get(),
        )
    };
    (ITER_NEXT, $id:expr, $pair:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::IterNext(
                $id,
                $pair.clone().map(|(k, v)| (k.to_vec(), v.to_vec())),
            ),
            risingwave_common::hm_trace::task_local_get(),
        )
    };
    (SYNC, $epoch:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::Sync($epoch),
            risingwave_common::hm_trace::TraceLocalId::None,
        )
    };
    (SEAL, $epoch:ident, $check_point:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::Seal($epoch, $check_point),
            risingwave_common::hm_trace::task_local_get(),
        )
    };
    (VERSION) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::UpdateVersion(),
            risingwave_common::hm_trace::TraceLocalId::None,
        )
    };
    (METAMSG, $resp:ident) => {
        $crate::collector::TraceSpan::new_global_op(
            $crate::record::Operation::MetaMessage($crate::record::TraceSubResp($resp.clone())),
            risingwave_common::hm_trace::TraceLocalId::None,
        )
    };
}
