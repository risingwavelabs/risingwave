// Copyright 2023 RisingWave Labs
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

use rand::{Rng, SeedableRng};

use crate::array::stream_chunk::Op;
use crate::array::{ArrayBuilder, ArrayImpl, I64ArrayBuilder};
use crate::buffer::Bitmap;

pub fn gen_legal_stream_chunk(
    bitmap: Option<&Bitmap>,
    chunk_size: usize,
    append_only: bool,
    seed: u64,
) -> (Vec<Op>, ArrayImpl) {
    let mut data_builder = I64ArrayBuilder::new(chunk_size);
    let mut ops: Vec<Op> = vec![];
    let mut cur_data: Vec<i64> = vec![];
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    if let Some(bitmap) = bitmap {
        for i in 0..chunk_size {
            // SAFETY(value_at_unchecked): the idx is always in bound.
            unsafe {
                if bitmap.is_set_unchecked(i) {
                    let op = if append_only || cur_data.is_empty() || rng.gen() {
                        Op::Insert
                    } else {
                        Op::Delete
                    };
                    ops.push(op);
                    if op == Op::Insert {
                        let value = rng.gen::<i32>() as i64;
                        data_builder.append(Some(value));
                        cur_data.push(value);
                    } else {
                        let idx = rng.gen_range(0..cur_data.len());
                        data_builder.append(Some(cur_data[idx]));
                        cur_data.remove(idx);
                    }
                } else {
                    ops.push(Op::Insert);
                    data_builder.append(Some(1234567890));
                }
            }
        }
    } else {
        for _ in 0..chunk_size {
            let op = if append_only || cur_data.is_empty() || rng.gen() {
                Op::Insert
            } else {
                Op::Delete
            };
            ops.push(op);
            if op == Op::Insert {
                let value = rng.gen::<i32>() as i64;
                data_builder.append(Some(value));
                cur_data.push(value);
            } else {
                let idx = rng.gen_range(0..cur_data.len());
                data_builder.append(Some(cur_data[idx]));
                cur_data.remove(idx);
            }
        }
    }
    (ops, data_builder.finish().into())
}
