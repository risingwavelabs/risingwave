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

mod chunked_data;
mod distributed_lookup_join;
pub mod hash_join;
pub mod local_lookup_join;
mod lookup_join_base;
pub mod nested_loop_join;

pub use chunked_data::*;
pub use distributed_lookup_join::*;
pub use hash_join::*;
use itertools::Itertools;
pub use local_lookup_join::*;
pub use lookup_join_base::*;
pub use nested_loop_join::*;
use risingwave_common::array::{DataChunk, RowRef};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::{AsOfJoinDesc, AsOfJoinInequalityType, JoinType as PbJoinType};

use crate::error::Result;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub enum JoinType {
    #[default]
    Inner,
    LeftOuter,
    /// Semi join when probe side should output when matched
    LeftSemi,
    /// Anti join when probe side should not output when matched
    LeftAnti,
    RightOuter,
    /// Semi join when build side should output when matched
    RightSemi,
    /// Anti join when build side should output when matched
    RightAnti,
    FullOuter,
    AsOfInner,
    AsOfLeftOuter,
}

impl JoinType {
    pub fn from_prost(prost: PbJoinType) -> Self {
        match prost {
            PbJoinType::Inner => JoinType::Inner,
            PbJoinType::LeftOuter => JoinType::LeftOuter,
            PbJoinType::LeftSemi => JoinType::LeftSemi,
            PbJoinType::LeftAnti => JoinType::LeftAnti,
            PbJoinType::RightOuter => JoinType::RightOuter,
            PbJoinType::RightSemi => JoinType::RightSemi,
            PbJoinType::RightAnti => JoinType::RightAnti,
            PbJoinType::FullOuter => JoinType::FullOuter,
            PbJoinType::AsofInner => JoinType::AsOfInner,
            PbJoinType::AsofLeftOuter => JoinType::AsOfLeftOuter,
            PbJoinType::Unspecified => {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
impl JoinType {
    #![allow(dead_code)]

    fn keep_all(self) -> bool {
        matches!(
            self,
            JoinType::FullOuter | JoinType::LeftOuter | JoinType::RightOuter | JoinType::Inner
        )
    }

    fn keep_left(self) -> bool {
        matches!(self, JoinType::LeftAnti | JoinType::LeftSemi)
    }

    fn keep_right(self) -> bool {
        matches!(self, JoinType::RightAnti | JoinType::RightSemi)
    }
}

#[derive(Clone, Debug)]
pub enum AsOfInequalityType {
    Le,
    Lt,
    Ge,
    Gt,
}

#[derive(Clone, Debug)]
pub struct AsOfDesc {
    pub left_idx: usize,
    pub right_idx: usize,
    pub inequality_type: AsOfInequalityType,
}

impl AsOfDesc {
    pub fn from_protobuf(desc_proto: &AsOfJoinDesc) -> crate::error::Result<Self> {
        let typ = match desc_proto.inequality_type() {
            AsOfJoinInequalityType::AsOfInequalityTypeLt => AsOfInequalityType::Lt,
            AsOfJoinInequalityType::AsOfInequalityTypeLe => AsOfInequalityType::Le,
            AsOfJoinInequalityType::AsOfInequalityTypeGt => AsOfInequalityType::Gt,
            AsOfJoinInequalityType::AsOfInequalityTypeGe => AsOfInequalityType::Ge,
            AsOfJoinInequalityType::AsOfInequalityTypeUnspecified => {
                bail!("unspecified AsOf join inequality type")
            }
        };
        Ok(Self {
            left_idx: desc_proto.left_idx as usize,
            right_idx: desc_proto.right_idx as usize,
            inequality_type: typ,
        })
    }
}

/// The layout be like:
///
/// [ `left` chunk     |  `right` chunk     ]
///
/// # Arguments
///
/// * `left` Data chunk padded to the left half of result data chunk..
/// * `right` Data chunk padded to the right half of result data chunk.
///
/// Note: Use this function with careful: It is not designed to be a general concatenate of two
/// chunk: Usually one side should be const row chunk and the other side is normal chunk.
/// Currently only feasible to use in join executor.
/// If two normal chunk, the result is undefined.
fn concatenate(left: &DataChunk, right: &DataChunk) -> Result<DataChunk> {
    assert_eq!(left.capacity(), right.capacity());
    let mut concated_columns = Vec::with_capacity(left.columns().len() + right.columns().len());
    concated_columns.extend_from_slice(left.columns());
    concated_columns.extend_from_slice(right.columns());
    // Only handle one side is constant row chunk: One of visibility must be None.
    let vis = match (left.is_compacted(), right.is_compacted()) {
        (true, _) => right.visibility().clone(),
        (_, true) => left.visibility().clone(),
        (false, false) => {
            panic!("The concatenate behaviour of two chunk with visibility is undefined")
        }
    };
    let data_chunk = DataChunk::new(concated_columns, vis);
    Ok(data_chunk)
}

/// Create constant data chunk (one tuple repeat `num_tuples` times).
fn convert_datum_refs_to_chunk(
    datum_refs: &[DatumRef<'_>],
    num_tuples: usize,
    data_types: &[DataType],
) -> Result<DataChunk> {
    let mut output_array_builders: Vec<_> = data_types
        .iter()
        .map(|data_type| data_type.create_array_builder(num_tuples))
        .collect();
    for _i in 0..num_tuples {
        for (builder, datum_ref) in output_array_builders.iter_mut().zip_eq_fast(datum_refs) {
            builder.append(*datum_ref);
        }
    }

    // Finish each array builder and get Column.
    let result_columns = output_array_builders
        .into_iter()
        .map(|b| b.finish().into())
        .collect();

    Ok(DataChunk::new(result_columns, num_tuples))
}

/// Create constant data chunk (one tuple repeat `num_tuples` times).
fn convert_row_to_chunk(
    row_ref: &RowRef<'_>,
    num_tuples: usize,
    data_types: &[DataType],
) -> Result<DataChunk> {
    let datum_refs = row_ref.iter().collect_vec();
    convert_datum_refs_to_chunk(&datum_refs, num_tuples, data_types)
}

#[cfg(test)]
mod tests {

    use risingwave_common::array::{Array, ArrayBuilder, DataChunk, PrimitiveArrayBuilder};
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarRefImpl};

    use crate::executor::join::{concatenate, convert_datum_refs_to_chunk};

    #[test]
    fn test_concatenate() {
        let num_of_columns: usize = 2;
        let length = 5;
        let mut columns = vec![];
        for i in 0..num_of_columns {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length);
            for _ in 0..length {
                builder.append(Some(i as i32));
            }
            let arr = builder.finish();
            columns.push(arr.into_ref())
        }
        let chunk1 = DataChunk::new(columns.clone(), length);
        let visibility = Bitmap::from_bool_slice(&[true, false, true, false, false]);
        let chunk2 = DataChunk::new(columns.clone(), visibility.clone());
        let chunk = concatenate(&chunk1, &chunk2).unwrap();
        assert_eq!(chunk.capacity(), chunk1.capacity());
        assert_eq!(chunk.capacity(), chunk2.capacity());
        assert_eq!(chunk.columns().len(), chunk1.columns().len() * 2);
        assert_eq!(chunk.visibility(), &visibility);
    }

    /// Test the function of convert row into constant row chunk (one row repeat multiple times).
    #[test]
    fn test_convert_row_to_chunk() {
        let row = vec![Some(ScalarRefImpl::Int32(3))];
        let probe_side_schema = Schema {
            fields: vec![Field::unnamed(DataType::Int32)],
        };
        let const_row_chunk =
            convert_datum_refs_to_chunk(&row, 5, &probe_side_schema.data_types()).unwrap();
        assert_eq!(const_row_chunk.capacity(), 5);
        assert_eq!(
            const_row_chunk.row_at(2).0.datum_at(0),
            Some(ScalarRefImpl::Int32(3))
        );
    }
}
