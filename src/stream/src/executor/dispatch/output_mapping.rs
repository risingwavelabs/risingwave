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

use risingwave_common::util::iter_util::ZipEqFast as _;
use risingwave_pb::stream_plan::PbDispatchOutputMapping;

use crate::executor::prelude::*;

#[derive(Debug)]
pub enum DispatchOutputMapping {
    Simple(Vec<usize>),
    TypeMapping {
        indices: Vec<usize>,
        types: Vec<Option<(DataType, DataType)>>,
    },
}

impl DispatchOutputMapping {
    pub(super) fn from_protobuf(proto: PbDispatchOutputMapping) -> Self {
        let indices = proto.indices.into_iter().map(|i| i as usize).collect();

        if proto.types.is_empty() {
            Self::Simple(indices)
        } else {
            let types = (proto.types.into_iter())
                .map(|t| {
                    if t.upstream.is_some() {
                        Some((t.upstream.unwrap().into(), t.downstream.unwrap().into()))
                    } else {
                        None
                    }
                })
                .collect();
            Self::TypeMapping { indices, types }
        }
    }

    pub(super) fn apply(&self, chunk: StreamChunk) -> StreamChunk {
        match self {
            Self::Simple(indices) => {
                if indices.len() < chunk.columns().len() {
                    chunk.project(indices).eliminate_adjacent_noop_update()
                } else {
                    chunk.project(indices)
                }
            }

            Self::TypeMapping { indices, types } => {
                let (ops, columns, visibility) = chunk.into_inner();

                let mut new_columns = Vec::with_capacity(indices.len());
                for (i, t) in indices.iter().zip_eq_fast(types) {
                    let mut column = columns[*i].clone();

                    if let Some((from_type, into_type)) = t {
                        let mut builder = into_type.create_array_builder(column.len());
                        for (datum, vis) in column.iter().zip_eq_fast(visibility.iter()) {
                            if !vis {
                                builder.append_null();
                            } else {
                                let datum = type_mapping::do_map(datum, from_type, into_type);
                                builder.append(datum);
                            }
                        }
                        column = builder.finish().into();
                    }

                    new_columns.push(column);
                }

                StreamChunk::with_visibility(ops, new_columns, visibility)
            }
        }
    }

    pub(super) fn apply_watermark(&self, watermark: Watermark) -> Option<Watermark> {
        match self {
            Self::Simple(indices) => watermark.transform_with_indices(indices),
            // Type change is only supported on composite types, while watermark must be a simple type.
            // So we simply ignore type mapping here.
            Self::TypeMapping { indices, types: _ } => watermark.transform_with_indices(indices),
        }
    }
}

mod type_mapping {
    use risingwave_common::types::{
        DataType, DatumCow, DatumRef, ListValue, MapValue, ScalarImpl, StructValue, ToOwnedDatum,
        data_types,
    };
    use risingwave_common::util::iter_util::ZipEqFast;

    pub fn do_map<'a>(
        datum: DatumRef<'a>,
        from_type: &DataType,
        into_type: &DataType,
    ) -> DatumCow<'a> {
        let Some(scalar) = datum else {
            return DatumCow::NULL;
        };

        if from_type == into_type {
            return DatumCow::Borrowed(datum);
        }

        match (from_type, into_type) {
            (data_types::simple!(), data_types::simple!()) => DatumCow::Borrowed(Some(scalar)),

            (DataType::List(from_inner_type), DataType::List(into_inner_type)) => {
                let list = scalar.into_list();

                let mut builder = into_inner_type.create_array_builder(list.len());
                for datum in list.iter() {
                    let datum = do_map(datum, from_inner_type, into_inner_type);
                    builder.append(datum);
                }
                let list = ListValue::new(builder.finish());

                DatumCow::Owned(Some(ScalarImpl::List(list)))
            }

            (DataType::Map(from_map_type), DataType::Map(into_map_type)) => {
                assert_eq!(
                    from_map_type.key(),
                    into_map_type.key(),
                    "key type should not be changed"
                );

                let map = scalar.into_map();
                let (keys, values) = map.into_kv();

                let mut value_builder = into_map_type.value().create_array_builder(map.len());
                for value in values.iter() {
                    let value = do_map(value, from_map_type.value(), into_map_type.value());
                    value_builder.append(value);
                }
                let values = ListValue::new(value_builder.finish());

                let map = MapValue::try_from_kv(keys.to_owned(), values).unwrap();

                DatumCow::Owned(Some(ScalarImpl::Map(map)))
            }

            (DataType::Struct(from_struct_type), DataType::Struct(into_struct_type)) => {
                let struct_value = scalar.into_struct();
                let mut fields = Vec::with_capacity(into_struct_type.len());

                for (id, into_field_type) in into_struct_type
                    .ids()
                    .unwrap()
                    .zip_eq_fast(into_struct_type.types())
                {
                    let index = from_struct_type.ids().unwrap().position(|x| x == id);

                    let field = if let Some(index) = index {
                        let from_field_type = from_struct_type.type_at(index);
                        let field = struct_value.field_at(index);
                        do_map(field, from_field_type, into_field_type).to_owned_datum()
                    } else {
                        // NULL
                        None
                    };

                    fields.push(field);
                }

                let struct_value = StructValue::new(fields);

                DatumCow::Owned(Some(ScalarImpl::Struct(struct_value)))
            }

            _ => panic!("mismatched types: {from_type:?} -> {into_type:?}"),
        }
    }
}
