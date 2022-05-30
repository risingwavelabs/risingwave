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

use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};

use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;
use risingwave_pb::data::{Array as ProstArray, ArrayType as ProstArrayType, ListArrayData};

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayIterator, ArrayMeta, RowRef,
    NULL_VAL_FOR_HASH,
};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;
use crate::types::{
    display_datum_ref, to_datum_ref, DataType, Datum, DatumRef, Scalar, ScalarRefImpl,
};

/// This is a naive implementation of list array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct ListArrayBuilder {
    bitmap: BitmapBuilder,
    offsets: Vec<usize>,
    value: Box<ArrayBuilderImpl>,
    value_type: DataType,
    len: usize,
}

impl ArrayBuilder for ListArrayBuilder {
    type ArrayType = ListArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Result<Self> {
        panic!("Must use with_meta.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Result<Self> {
        Self::with_meta(
            capacity,
            ArrayMeta::List {
                // Default datatype
                datatype: Box::new(DataType::Int16),
            },
        )
    }

    fn with_meta(capacity: usize, meta: ArrayMeta) -> Result<Self> {
        if let ArrayMeta::List { datatype } = meta {
            Ok(Self {
                bitmap: BitmapBuilder::with_capacity(capacity),
                offsets: vec![0],
                value: Box::new(datatype.create_array_builder(capacity)?),
                value_type: *datatype,
                len: 0,
            })
        } else {
            panic!("must be ArrayMeta::List");
        }
    }

    fn append(&mut self, value: Option<ListRef<'_>>) -> Result<()> {
        match value {
            None => {
                self.bitmap.append(false);
                let last = *self.offsets.last().unwrap();
                self.offsets.push(last);
            }
            Some(v) => {
                self.bitmap.append(true);
                let last = *self.offsets.last().unwrap();
                let values_ref = v.values_ref();
                self.offsets.push(last + values_ref.len());
                for f in values_ref {
                    self.value.append_datum_ref(f)?;
                }
            }
        }
        self.len += 1;
        Ok(())
    }

    fn append_array(&mut self, other: &ListArray) -> Result<()> {
        self.bitmap.append_bitmap(&other.bitmap);
        let last = *self.offsets.last().unwrap();
        self.offsets
            .append(&mut other.offsets[1..].iter().map(|o| *o + last).collect());
        self.value.append_array(&other.value)?;
        self.len += other.len();
        Ok(())
    }

    fn finish(mut self) -> Result<ListArray> {
        Ok(ListArray {
            bitmap: self.bitmap.finish(),
            offsets: self.offsets,
            value: Box::new(self.value.finish()?),
            value_type: self.value_type,
            len: self.len,
        })
    }
}

impl ListArrayBuilder {
    pub fn append_row_ref(&mut self, row: RowRef) -> Result<()> {
        self.bitmap.append(true);
        let last = *self.offsets.last().unwrap();
        self.offsets.push(last + row.size());
        self.len += 1;
        row.values()
            .try_for_each(|v| self.value.append_datum_ref(v))
    }
}

/// This is a naive implementation of list array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct ListArray {
    bitmap: Bitmap,
    offsets: Vec<usize>,
    value: Box<ArrayImpl>,
    value_type: DataType,
    len: usize,
}

impl Array for ListArray {
    type Builder = ListArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = ListValue;
    type RefItem<'a> = ListRef<'a>;

    fn value_at(&self, idx: usize) -> Option<ListRef<'_>> {
        if !self.is_null(idx) {
            Some(ListRef::Indexed { arr: self, idx })
        } else {
            None
        }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<ListRef<'_>> {
        if !self.is_null_unchecked(idx) {
            Some(ListRef::Indexed { arr: self, idx })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> ProstArray {
        let value = self.value.to_protobuf();
        ProstArray {
            array_type: ProstArrayType::List as i32,
            struct_array_data: None,
            list_array_data: Some(Box::new(ListArrayData {
                offsets: self.offsets.iter().map(|u| *u as u32).collect(),
                value: Some(Box::new(value)),
                value_type: Some(self.value_type.to_protobuf()),
            })),
            null_bitmap: Some(self.bitmap.to_protobuf()),
            values: vec![],
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    fn hash_at<H: std::hash::Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.value.hash_at(idx, state)
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> Result<super::ArrayBuilderImpl> {
        let array_builder = ListArrayBuilder::with_meta(
            capacity,
            ArrayMeta::List {
                datatype: Box::new(self.value_type.clone()),
            },
        )?;
        Ok(ArrayBuilderImpl::List(array_builder))
    }
}

impl ListArray {
    pub fn from_protobuf(array: &ProstArray) -> Result<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a list array"
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
        let cardinality = bitmap.len();
        let array_data = array.get_list_array_data()?.to_owned();
        let value = ArrayImpl::from_protobuf(array_data.value.as_ref().unwrap(), cardinality)?;
        let arr = ListArray {
            bitmap,
            offsets: array_data.offsets.iter().map(|u| *u as usize).collect(),
            value: Box::new(value),
            value_type: DataType::from(&array_data.value_type.unwrap()),
            len: cardinality,
        };
        Ok(arr.into())
    }

    // Used for testing purposes
    pub fn from_slices(
        null_bitmap: &[bool],
        values: Vec<Option<ArrayImpl>>,
        value_type: DataType,
    ) -> Result<ListArray> {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::try_from(null_bitmap.to_vec())?;
        let mut offsets = vec![0];
        let mut values = values.into_iter().peekable();
        let mut builderimpl = values.peek().unwrap().as_ref().unwrap().create_builder(0)?;
        values.try_for_each(|i| match i {
            Some(a) => {
                offsets.push(a.len());
                builderimpl.append_array(&a)
            }
            None => {
                offsets.push(0);
                Ok(())
            }
        })?;
        offsets.iter_mut().fold(0, |acc, x| {
            *x += acc;
            *x
        });
        Ok(ListArray {
            bitmap,
            offsets,
            value: Box::new(builderimpl.finish()?),
            value_type,
            len: cardinality,
        })
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<ListValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

#[derive(Clone, Debug, Eq, Default, PartialEq, Hash)]
pub struct ListValue {
    values: Vec<Datum>,
}

impl fmt::Display for ListValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Example of ListValue display: ARRAY[1, 2]
        write!(
            f,
            "ARRAY[{}]",
            self.values.iter().map(|v| v.as_ref().unwrap()).format(", ")
        )
    }
}

impl PartialOrd for ListValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref().partial_cmp(&other.as_scalar_ref())
    }
}

impl Ord for ListValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl ListValue {
    pub fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Datum] {
        &self.values
    }
}

#[derive(Copy, Clone)]
pub enum ListRef<'a> {
    Indexed { arr: &'a ListArray, idx: usize },
    ValueRef { val: &'a ListValue },
}

impl<'a> ListRef<'a> {
    pub fn values_ref(&self) -> Vec<DatumRef<'a>> {
        match self {
            ListRef::Indexed { arr, idx } => (arr.offsets[*idx]..arr.offsets[*idx + 1])
                .map(|o| arr.value.value_at(o))
                .collect(),
            ListRef::ValueRef { val } => val.values.iter().map(to_datum_ref).collect(),
        }
    }

    pub fn value_at(&self, index: usize) -> Result<DatumRef<'a>> {
        match self {
            ListRef::Indexed { arr, .. } => {
                if index <= arr.value.len() {
                    Ok(arr.value.value_at(index - 1))
                } else {
                    Ok(None)
                }
            }
            ListRef::ValueRef { val } => {
                if let Some(datum) = val.values().iter().nth(index - 1) {
                    Ok(to_datum_ref(datum))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl Hash for ListRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ListRef::Indexed { arr, idx } => arr.hash_at(*idx, state),
            ListRef::ValueRef { val } => val.hash(state),
        }
    }
}

impl PartialEq for ListRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.values_ref().eq(&other.values_ref())
    }
}

impl PartialOrd for ListRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let l = self.values_ref();
        let r = other.values_ref();
        let it = l.iter().zip_longest(r.iter()).find_map(|e| match e {
            Both(ls, rs) => {
                let ord = cmp_list_value(ls, rs);
                if let Ordering::Equal = ord {
                    None
                } else {
                    Some(ord)
                }
            }
            Left(_) => Some(Ordering::Greater),
            Right(_) => Some(Ordering::Less),
        });
        it.or(Some(Ordering::Equal))
    }
}

fn cmp_list_value(l: &Option<ScalarRefImpl>, r: &Option<ScalarRefImpl>) -> Ordering {
    match (l, r) {
        // Comparability check was performed by frontend beforehand.
        (Some(sl), Some(sr)) => sl.partial_cmp(sr).unwrap(),
        // Nulls are larger than everything, ARRAY[1, null] > ARRAY[1, 2] for example.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Debug for ListRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ListRef::Indexed { arr, idx } => (arr.offsets[*idx]..arr.offsets[*idx + 1])
                .try_for_each(|idx| arr.value.value_at(idx).fmt(f)),
            ListRef::ValueRef { val } => write!(f, "{:?}", val),
        }
    }
}

impl Display for ListRef<'_> {
    // This function will be invoked when pgwire prints a list value in string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = self.values_ref().iter().map(display_datum_ref).join(",");
        write!(f, "{{{}}}", values)
    }
}

impl Eq for ListRef<'_> {}

impl Ord for ListRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order between two lists is deterministic.
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::{assert_gt, assert_lt};

    use super::*;
    use crate::{array, empty_array, try_match_expand};

    #[test]
    fn test_list_with_values() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[true, false, true, true],
            vec![
                Some(array! { I32Array, [Some(12), Some(-7), Some(25)] }.into()),
                None,
                Some(array! { I32Array, [Some(0), Some(-127), Some(127), Some(50)] }.into()),
                Some(empty_array! { I32Array }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();
        let actual = ListArray::from_protobuf(&arr.to_protobuf()).unwrap();
        let tmp = ArrayImpl::List(arr);
        assert_eq!(tmp, actual);

        let arr = try_match_expand!(actual, ArrayImpl::List).unwrap();
        let list_values = arr.values_vec();
        assert_eq!(
            list_values,
            vec![
                Some(ListValue::new(vec![
                    Some(ScalarImpl::Int32(12)),
                    Some(ScalarImpl::Int32(-7)),
                    Some(ScalarImpl::Int32(25)),
                ])),
                None,
                Some(ListValue::new(vec![
                    Some(ScalarImpl::Int32(0)),
                    Some(ScalarImpl::Int32(-127)),
                    Some(ScalarImpl::Int32(127)),
                    Some(ScalarImpl::Int32(50)),
                ])),
                Some(ListValue::new(vec![])),
            ]
        );

        let mut builder = ListArrayBuilder::with_meta(
            4,
            ArrayMeta::List {
                datatype: Box::new(DataType::Int32),
            },
        )
        .unwrap();
        list_values.iter().for_each(|v| {
            builder
                .append(v.as_ref().map(|s| s.as_scalar_ref()))
                .unwrap()
        });
        let arr = builder.finish().unwrap();
        assert_eq!(arr.values_vec(), list_values);

        let part1 = ListArray::from_slices(
            &[true, false],
            vec![
                Some(array! { I32Array, [Some(12), Some(-7), Some(25)] }.into()),
                None,
            ],
            DataType::Int32,
        )
        .unwrap();

        let part2 = ListArray::from_slices(
            &[true, true],
            vec![
                Some(array! { I32Array, [Some(0), Some(-127), Some(127), Some(50)] }.into()),
                Some(empty_array! { I32Array }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        let mut builder = ListArrayBuilder::with_meta(
            4,
            ArrayMeta::List {
                datatype: Box::new(DataType::Int32),
            },
        )
        .unwrap();
        builder.append_array(&part1).unwrap();
        builder.append_array(&part2).unwrap();

        assert_eq!(arr.values_vec(), builder.finish().unwrap().values_vec());
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_list_create_builder() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[true],
            vec![Some(
                array! { F32Array, [Some(2.0), Some(42.0), Some(1.0)] }.into(),
            )],
            DataType::Float32,
        )
        .unwrap();
        let builder = arr.create_builder(0).unwrap();
        let arr2 = try_match_expand!(builder.finish().unwrap(), ArrayImpl::List).unwrap();
        assert_eq!(arr.array_meta(), arr2.array_meta());
    }

    #[test]
    fn test_list_nested_layout() {
        use crate::array::*;

        let listarray1 = ListArray::from_slices(
            &[true, true],
            vec![
                Some(array! { I32Array, [Some(1), Some(2)] }.into()),
                Some(array! { I32Array, [Some(3), Some(4)] }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        let listarray2 = ListArray::from_slices(
            &[true, false, true],
            vec![
                Some(array! { I32Array, [Some(5), Some(6), Some(7)] }.into()),
                None,
                Some(array! { I32Array, [Some(8)] }.into()),
            ],
            DataType::Int32,
        )
        .unwrap();

        let listarray3 = ListArray::from_slices(
            &[true],
            vec![Some(array! { I32Array, [Some(9), Some(10)] }.into())],
            DataType::Int32,
        )
        .unwrap();

        let nestarray = ListArray::from_slices(
            &[true, true, true],
            vec![
                Some(listarray1.into()),
                Some(listarray2.into()),
                Some(listarray3.into()),
            ],
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
        )
        .unwrap();
        let actual = ListArray::from_protobuf(&nestarray.to_protobuf()).unwrap();
        assert_eq!(ArrayImpl::List(nestarray), actual);

        let nestarray = try_match_expand!(actual, ArrayImpl::List).unwrap();
        let nested_list_values = nestarray.values_vec();
        assert_eq!(
            nested_list_values,
            vec![
                Some(ListValue::new(vec![
                    Some(ScalarImpl::List(ListValue::new(vec![
                        Some(ScalarImpl::Int32(1)),
                        Some(ScalarImpl::Int32(2)),
                    ]))),
                    Some(ScalarImpl::List(ListValue::new(vec![
                        Some(ScalarImpl::Int32(3)),
                        Some(ScalarImpl::Int32(4)),
                    ]))),
                ])),
                Some(ListValue::new(vec![
                    Some(ScalarImpl::List(ListValue::new(vec![
                        Some(ScalarImpl::Int32(5)),
                        Some(ScalarImpl::Int32(6)),
                        Some(ScalarImpl::Int32(7)),
                    ]))),
                    None,
                    Some(ScalarImpl::List(ListValue::new(vec![Some(
                        ScalarImpl::Int32(8)
                    ),]))),
                ])),
                Some(ListValue::new(vec![Some(ScalarImpl::List(
                    ListValue::new(vec![
                        Some(ScalarImpl::Int32(9)),
                        Some(ScalarImpl::Int32(10)),
                    ])
                )),])),
            ]
        );

        let mut builder = ListArrayBuilder::with_meta(
            3,
            ArrayMeta::List {
                datatype: Box::new(DataType::List {
                    datatype: Box::new(DataType::Int32),
                }),
            },
        )
        .unwrap();
        nested_list_values.iter().for_each(|v| {
            builder
                .append(v.as_ref().map(|s| s.as_scalar_ref()))
                .unwrap()
        });
        let nestarray = builder.finish().unwrap();
        assert_eq!(nestarray.values_vec(), nested_list_values);
    }

    #[test]
    fn test_list_value_cmp() {
        // ARRAY[1, 1] < ARRAY[1, 2, 1]
        assert_lt!(
            ListValue::new(vec![Some(1.into()), Some(1.into())]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // ARRAY[1, 2] < ARRAY[1, 2, 1]
        assert_lt!(
            ListValue::new(vec![Some(1.into()), Some(2.into())]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // ARRAY[1, 3] > ARRAY[1, 2, 1]
        assert_gt!(
            ListValue::new(vec![Some(1.into()), Some(3.into())]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // null > 1
        assert_eq!(
            cmp_list_value(&None, &Some(ScalarRefImpl::Int32(1))),
            Ordering::Greater
        );
        // ARRAY[1, 2, null] > ARRAY[1, 2, 1]
        assert_gt!(
            ListValue::new(vec![Some(1.into()), Some(2.into()), None]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(1.into())]),
        );
        // Null value in first ARRAY results into a Greater ordering regardless of the smaller ARRAY
        // length. ARRAY[1, null] > ARRAY[1, 2, 3]
        assert_gt!(
            ListValue::new(vec![Some(1.into()), None]),
            ListValue::new(vec![Some(1.into()), Some(2.into()), Some(3.into())]),
        );
        // ARRAY[1, null] == ARRAY[1, null]
        assert_eq!(
            ListValue::new(vec![Some(1.into()), None]),
            ListValue::new(vec![Some(1.into()), None]),
        );
    }

    #[test]
    fn test_list_ref_display() {
        let v = ListValue::new(vec![Some(1.into()), None]);
        let r = ListRef::ValueRef { val: &v };
        assert_eq!("{1,NULL}".to_string(), format!("{}", r));
    }
}
