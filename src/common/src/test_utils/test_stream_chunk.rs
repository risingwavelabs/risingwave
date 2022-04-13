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

use crate::array::column::Column;
use crate::array::{F32Array, I32Array, I64Array, Op, Row, StreamChunk};
use crate::catalog::{Field, Schema};
use crate::types::{DataType, Datum};

pub trait TestStreamChunk {
    fn stream_chunk() -> StreamChunk;

    fn cardinality() -> usize;

    fn schema() -> Schema;

    fn pk_indices() -> Vec<usize> {
        unimplemented!()
    }

    fn data_types() -> Vec<DataType> {
        Self::schema().data_types()
    }

    fn op_at(idx: usize) -> Op;

    fn row_at(idx: usize) -> Row;

    fn row_with_op_at(idx: usize) -> (Op, Row) {
        (Self::op_at(idx), Self::row_at(idx))
    }

    fn value_at(row_idx: usize, col_idx: usize) -> Datum {
        Self::row_at(row_idx)[col_idx].clone()
    }
}

pub struct WhatEverStreamChunk;

impl TestStreamChunk for WhatEverStreamChunk {
    fn stream_chunk() -> StreamChunk {
        let ops = vec![
            Op::Insert,
            Op::Delete,
            Op::Insert,
            Op::UpdateDelete,
            Op::UpdateInsert,
        ];
        let visibility = Some(vec![true, true, false, true, true].try_into().unwrap());
        let col1 = Column::from(crate::array!(
            I32Array,
            [Some(1), Some(2), None, Some(3), Some(4)],
        ));
        let col2 = Column::from(crate::array!(
            F32Array,
            [Some(4.0), None, Some(3.5), Some(2.2), Some(1.8)],
        ));
        let col3 = Column::from(crate::array!(
            I64Array,
            [Some(5), Some(6), Some(7), Some(8), Some(9)],
        ));
        let cols = vec![col1, col2, col3];
        StreamChunk::new(ops, cols, visibility)
    }

    fn cardinality() -> usize {
        4
    }

    fn pk_indices() -> Vec<usize> {
        vec![0]
    }

    fn schema() -> Schema {
        let field1 = Field::with_name(DataType::Int32, "pk");
        let field2 = Field::with_name(DataType::Float32, "v2");
        let field3 = Field::with_name(DataType::Int64, "v3");
        let fields = vec![field1, field2, field3];
        Schema::new(fields)
    }

    fn op_at(idx: usize) -> Op {
        match idx {
            0 => Op::Insert,
            1 => Op::Delete,
            2 => Op::UpdateDelete,
            3 => Op::UpdateInsert,
            _ => unreachable!(),
        }
    }

    fn row_at(idx: usize) -> Row {
        match idx {
            0 => Row(vec![
                Some(1i32.into()),
                Some(4.0f32.into()),
                Some(5i64.into()),
            ]),
            1 => Row(vec![Some(2i32.into()), None, Some(6i64.into())]),
            2 => Row(vec![
                Some(3i32.into()),
                Some(2.2f32.into()),
                Some(8i64.into()),
            ]),
            3 => Row(vec![
                Some(4i32.into()),
                Some(1.8f32.into()),
                Some(9i64.into()),
            ]),
            _ => unreachable!(),
        }
    }
}
