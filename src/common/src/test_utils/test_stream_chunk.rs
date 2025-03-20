// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use itertools::Itertools;

use crate::array::{I32Array, Op, StreamChunk};
use crate::catalog::{Field, Schema};
use crate::row::OwnedRow;
use crate::test_prelude::StreamChunkTestExt;
use crate::types::{DataType, Datum, ScalarImpl};

pub trait TestStreamChunk {
    fn stream_chunk(&self) -> StreamChunk;

    fn cardinality(&self) -> usize;

    fn schema(&self) -> Schema;

    fn pk_indices(&self) -> Vec<usize> {
        unimplemented!()
    }

    fn data_types(&self) -> Vec<DataType> {
        self.schema().data_types()
    }

    fn op_at(&self, idx: usize) -> Op;

    fn row_at(&self, idx: usize) -> OwnedRow;

    fn row_with_op_at(&self, idx: usize) -> (Op, OwnedRow) {
        (self.op_at(idx), self.row_at(idx))
    }

    fn value_at(&self, row_idx: usize, col_idx: usize) -> Datum {
        self.row_at(row_idx)[col_idx].clone()
    }
}

pub struct BigStreamChunk(StreamChunk);

impl BigStreamChunk {
    #[expect(clippy::if_same_then_else)]
    #[expect(clippy::needless_bool)]
    pub fn new(capacity: usize) -> Self {
        let ops = (0..capacity)
            .map(|i| {
                if i % 20 == 0 || i % 20 == 1 {
                    Op::UpdateDelete
                } else if i % 20 == 2 {
                    Op::UpdateInsert
                } else if i % 2 == 0 {
                    Op::Insert
                } else {
                    Op::Delete
                }
            })
            .collect_vec();

        let visibility = (0..capacity)
            .map(|i| {
                if i % 20 == 1 {
                    false
                } else if i % 20 == 10 {
                    false
                } else {
                    true
                }
            })
            .collect_vec()
            .into_iter()
            .collect();

        let col = Arc::new(I32Array::from_iter(std::iter::repeat_n(114_514, capacity)).into());

        let chunk = StreamChunk::with_visibility(ops, vec![col], visibility);

        Self(chunk)
    }
}

impl TestStreamChunk for BigStreamChunk {
    fn stream_chunk(&self) -> StreamChunk {
        self.0.clone()
    }

    fn cardinality(&self) -> usize {
        self.0.cardinality()
    }

    fn schema(&self) -> Schema {
        Schema::new(vec![Field::with_name(DataType::Int32, "v")])
    }

    fn op_at(&self, i: usize) -> Op {
        self.0.ops()[i]
    }

    fn row_at(&self, _idx: usize) -> OwnedRow {
        OwnedRow::new(vec![Some(ScalarImpl::Int32(114_514))])
    }
}

pub struct WhatEverStreamChunk;

impl TestStreamChunk for WhatEverStreamChunk {
    fn stream_chunk(&self) -> StreamChunk {
        StreamChunk::from_pretty(
            "   i    f       I
            +   1    4.0     5
            -   2    .       6
            +   .    3.5     7 D
            U-  3    2.2     8
            U+  4    1.8     9
        ",
        )
    }

    fn cardinality(&self) -> usize {
        4
    }

    fn pk_indices(&self) -> Vec<usize> {
        vec![0]
    }

    fn schema(&self) -> Schema {
        let field1 = Field::with_name(DataType::Int32, "pk");
        let field2 = Field::with_name(DataType::Float32, "v2");
        let field3 = Field::with_name(DataType::Int64, "v3");
        let fields = vec![field1, field2, field3];
        Schema::new(fields)
    }

    fn op_at(&self, idx: usize) -> Op {
        match idx {
            0 => Op::Insert,
            1 => Op::Delete,
            2 => Op::UpdateDelete,
            3 => Op::UpdateInsert,
            _ => unreachable!(),
        }
    }

    fn row_at(&self, idx: usize) -> OwnedRow {
        match idx {
            0 => OwnedRow::new(vec![
                Some(1i32.into()),
                Some(4.0f32.into()),
                Some(5i64.into()),
            ]),
            1 => OwnedRow::new(vec![Some(2i32.into()), None, Some(6i64.into())]),
            2 => OwnedRow::new(vec![
                Some(3i32.into()),
                Some(2.2f32.into()),
                Some(8i64.into()),
            ]),
            3 => OwnedRow::new(vec![
                Some(4i32.into()),
                Some(1.8f32.into()),
                Some(9i64.into()),
            ]),
            _ => unreachable!(),
        }
    }
}
