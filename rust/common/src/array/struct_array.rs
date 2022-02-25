use core::fmt;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_pb::data::{
    Array as ProstArray, ArrayType as ProstArrayType, DataType as ProstDataType, StructArrayData,
};

use super::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayIterator, ArrayMeta, NULL_VAL_FOR_HASH,
};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;
use crate::types::{DataType, Datum, DatumRef, Scalar, ScalarRefImpl};

/// This is a naive implementation of struct array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct StructArrayBuilder {
    bitmap: BitmapBuilder,
    children_array: Vec<ArrayBuilderImpl>,
    children_type: Arc<[DataType]>,
    len: usize,
}

impl ArrayBuilder for StructArrayBuilder {
    type ArrayType = StructArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Result<Self> {
        panic!("Must use new_with_meta.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Result<Self> {
        Self::new_with_meta(
            capacity,
            ArrayMeta::Struct {
                children: Arc::new([]),
            },
        )
    }

    fn new_with_meta(capacity: usize, meta: ArrayMeta) -> Result<Self> {
        if let ArrayMeta::Struct { children } = meta {
            let children_array = children
                .iter()
                .map(|a| a.create_array_builder(capacity))
                .try_collect()?;
            Ok(Self {
                bitmap: BitmapBuilder::with_capacity(capacity),
                children_array,
                children_type: children,
                len: 0,
            })
        } else {
            panic!("must be ArrayMeta::Struct");
        }
    }

    fn append(&mut self, value: Option<StructRef<'_>>) -> Result<()> {
        match value {
            None => {
                self.bitmap.append(false);
                for child in &mut self.children_array {
                    child.append_datum_ref(None)?;
                }
            }
            Some(v) => {
                self.bitmap.append(true);
                let fields = v.fields_ref();
                assert_eq!(fields.len(), self.children_array.len());
                for (field_idx, f) in fields.into_iter().enumerate() {
                    self.children_array[field_idx].append_datum_ref(f)?;
                }
            }
        }
        self.len += 1;
        Ok(())
    }

    fn append_array(&mut self, other: &StructArray) -> Result<()> {
        self.bitmap.append_bitmap(&other.bitmap);
        self.children_array
            .iter_mut()
            .enumerate()
            .try_for_each(|(i, a)| a.append_array(&other.children[i]))?;
        self.len += other.len();
        Ok(())
    }

    fn finish(mut self) -> Result<StructArray> {
        let children = self
            .children_array
            .into_iter()
            .map(|b| b.finish())
            .try_collect()?;
        Ok(StructArray {
            bitmap: self.bitmap.finish(),
            children,
            children_type: self.children_type,
            len: self.len,
        })
    }
}

#[derive(Debug)]
pub struct StructArray {
    bitmap: Bitmap,
    children: Vec<ArrayImpl>,
    children_type: Arc<[DataType]>,
    len: usize,
}

impl Array for StructArray {
    type RefItem<'a> = StructRef<'a>;

    type OwnedItem = StructValue;

    type Builder = StructArrayBuilder;

    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<StructRef<'_>> {
        if !self.is_null(idx) {
            Some(StructRef::Indexed { arr: self, idx })
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

    fn to_protobuf(&self) -> Result<ProstArray> {
        let children_array = self
            .children
            .iter()
            .map(|a| a.to_protobuf())
            .collect::<Result<Vec<ProstArray>>>()?;
        let children_type = self
            .children_type
            .iter()
            .map(|t| t.to_protobuf())
            .collect::<Result<Vec<ProstDataType>>>()?;
        Ok(ProstArray {
            array_type: ProstArrayType::Struct as i32,
            struct_array_data: Some(StructArrayData {
                children_array,
                children_type,
            }),
            null_bitmap: Some(self.bitmap.to_protobuf()?),
            values: vec![],
        })
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn hash_at<H: std::hash::Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.children.iter().for_each(|a| a.hash_at(idx, state))
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> Result<super::ArrayBuilderImpl> {
        let array_builder = StructArrayBuilder::new_with_meta(
            capacity,
            ArrayMeta::Struct {
                children: self.children_type.clone(),
            },
        )?;
        Ok(ArrayBuilderImpl::Struct(array_builder))
    }

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::Struct {
            children: self.children_type.clone(),
        }
    }
}

impl StructArray {
    pub fn from_protobuf(array: &ProstArray) -> Result<ArrayImpl> {
        ensure!(
            array.values.is_empty(),
            "Must have no buffer in a struct array"
        );
        let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
        let cardinality = bitmap.len();
        let array_data = array.get_struct_array_data()?;
        let children = array_data
            .children_array
            .iter()
            .map(|child| ArrayImpl::from_protobuf(child, cardinality))
            .collect::<Result<Vec<ArrayImpl>>>()?;
        let children_type: Arc<[DataType]> = array_data
            .children_type
            .iter()
            .map(DataType::from)
            .collect::<Vec<DataType>>()
            .into();
        let arr = StructArray {
            bitmap,
            children,
            children_type,
            len: cardinality,
        };
        Ok(arr.into())
    }

    pub fn children_array_types(&self) -> &[DataType] {
        &self.children_type
    }

    #[cfg(test)]
    pub fn from_slices(
        null_bitmap: &[bool],
        children: Vec<ArrayImpl>,
        children_type: Vec<DataType>,
    ) -> Result<StructArray> {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::try_from(null_bitmap.to_vec())?;
        Ok(StructArray {
            bitmap,
            children_type: children_type.into(),
            len: cardinality,
            children,
        })
    }

    #[cfg(test)]
    pub fn values_vec(&self) -> Vec<Option<StructValue>> {
        use crate::types::ScalarRef;

        self.iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec()
    }
}

#[derive(Clone, Debug, Eq, Default, PartialEq, Hash)]
pub struct StructValue {
    fields: Vec<Datum>,
}

impl fmt::Display for StructValue {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl PartialOrd for StructValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_scalar_ref().partial_cmp(&other.as_scalar_ref())
    }
}

impl Ord for StructValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl StructValue {
    pub fn new(fields: Vec<Datum>) -> Self {
        Self { fields }
    }
}

#[derive(Copy, Clone)]
pub enum StructRef<'a> {
    Indexed { arr: &'a StructArray, idx: usize },
    ValueRef { val: &'a StructValue },
}

impl<'a> StructRef<'a> {
    pub fn fields_ref(&self) -> Vec<DatumRef<'a>> {
        match self {
            StructRef::Indexed { arr, idx } => {
                arr.children.iter().map(|a| a.value_at(*idx)).collect()
            }
            StructRef::ValueRef { val } => val
                .fields
                .iter()
                .map(|d| d.as_ref().map(|s| s.as_scalar_ref_impl()))
                .collect::<Vec<DatumRef<'a>>>(),
        }
    }
}

impl Hash for StructRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            StructRef::Indexed { arr, idx } => arr.hash_at(*idx, state),
            StructRef::ValueRef { val } => val.hash(state),
        }
    }
}

impl PartialEq for StructRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.fields_ref().eq(&other.fields_ref())
    }
}

impl PartialOrd for StructRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let l = self.fields_ref();
        let r = other.fields_ref();
        debug_assert_eq!(
            l.len(),
            r.len(),
            "Structs must have the same length to be compared"
        );
        let it = l.iter().enumerate().find_map(|(idx, v)| {
            let ord = cmp_struct_field(v, &r[idx]);
            if let Ordering::Equal = ord {
                None
            } else {
                Some(ord)
            }
        });
        it.or(Some(Ordering::Equal))
    }
}

fn cmp_struct_field(l: &Option<ScalarRefImpl>, r: &Option<ScalarRefImpl>) -> Ordering {
    match (l, r) {
        // Comparability check was performed by frontend beforehand.
        (Some(sl), Some(sr)) => sl.partial_cmp(sr).unwrap(),
        // Nulls are larger than everything, (1, null) > (1, 2) for example.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Debug for StructRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StructRef::Indexed { arr, idx } => arr
                .children
                .iter()
                .try_for_each(|a| a.value_at(*idx).fmt(f)),
            StructRef::ValueRef { val } => write!(f, "{:?}", val),
        }
    }
}

impl Display for StructRef<'_> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Eq for StructRef<'_> {}

impl Ord for StructRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The order between two structs is deterministic.
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::assert_gt;

    use super::*;
    use crate::{array, try_match_expand};

    // Empty struct is allowed in postgres.
    // `CREATE TYPE foo_empty as ();`, e.g.
    #[test]
    fn test_struct_new_empty() {
        let arr = StructArray::from_slices(&[true, false, true, false], vec![], vec![]).unwrap();
        let actual = StructArray::from_protobuf(&arr.to_protobuf().unwrap()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);
    }

    #[test]
    fn test_struct_with_fields() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[false, true, false, true],
            vec![
                array! { I32Array, [None, Some(1), None, Some(2)] }.into(),
                array! { F32Array, [None, Some(3.0), None, Some(4.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let actual = StructArray::from_protobuf(&arr.to_protobuf().unwrap()).unwrap();
        assert_eq!(ArrayImpl::Struct(arr), actual);

        let arr = try_match_expand!(actual, ArrayImpl::Struct).unwrap();
        let struct_values = arr.values_vec();
        assert_eq!(
            struct_values,
            vec![
                None,
                Some(StructValue::new(vec![
                    Some(ScalarImpl::Int32(1)),
                    Some(ScalarImpl::Float32(3.0.into())),
                ])),
                None,
                Some(StructValue::new(vec![
                    Some(ScalarImpl::Int32(2)),
                    Some(ScalarImpl::Float32(4.0.into())),
                ])),
            ]
        );

        let mut builder = StructArrayBuilder::new_with_meta(
            4,
            ArrayMeta::Struct {
                children: Arc::new([DataType::Int32, DataType::Float32]),
            },
        )
        .unwrap();
        struct_values.iter().for_each(|v| {
            builder
                .append(v.as_ref().map(|s| s.as_scalar_ref()))
                .unwrap()
        });
        let arr = builder.finish().unwrap();
        assert_eq!(arr.values_vec(), struct_values);
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_struct_create_builder() {
        use crate::array::*;
        let arr = StructArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1)] }.into(),
                array! { F32Array, [Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let builder = arr.create_builder(4).unwrap();
        let arr2 = try_match_expand!(builder.finish().unwrap(), ArrayImpl::Struct).unwrap();
        assert_eq!(arr.array_meta(), arr2.array_meta());
    }

    #[test]
    fn test_struct_value_cmp() {
        // (1, 2.0) > (1, 1.0)
        assert_gt!(
            StructValue::new(vec![Some(1.into()), Some(2.0.into())]),
            StructValue::new(vec![Some(1.into()), Some(1.0.into())]),
        );
        // null > 1
        assert_eq!(
            cmp_struct_field(&None, &Some(ScalarRefImpl::Int32(1))),
            Ordering::Greater
        );
        // (1, null, 3) > (1, 1.0, 2)
        assert_gt!(
            StructValue::new(vec![Some(1.into()), None, Some(3.into())]),
            StructValue::new(vec![Some(1.into()), Some(1.0.into()), Some(2.into())]),
        );
        // (1, null) == (1, null)
        assert_eq!(
            StructValue::new(vec![Some(1.into()), None]),
            StructValue::new(vec![Some(1.into()), None]),
        );
    }
}
