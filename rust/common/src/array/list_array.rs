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

/// This is a naive implementation of list array.
/// We will eventually move to a more efficient flatten implementation.
#[derive(Debug)]
pub struct ListArrayBuilder {
    bitmap: BitmapBuilder,
    children_array: Vec<ArrayBuilderImpl>,
    children_type: Arc<[DataType]>,
    len: usize,
}

impl ArrayBuilder for ListArrayBuilder {
    type ArrayType = ListArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Result<Self> {
        panic!("Must use new_with_meta.")
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Result<Self> {
        Self::new_with_meta(
            capacity,
            ArrayMeta::List {
                children: Arc::new([]),
            },
        )
    }

    fn new_with_meta(capacity: usize, meta: ArrayMeta) -> Result<Self> {
        if let ArrayMeta::List { children } = meta {
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
            panic!("must be ArrayMeta::List");
        }
    }

    fn append(&mut self, value: Option<ListRef<'_>>) -> Result<()> {
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

    fn append_array(&mut self, other: &ListArray) -> Result<()> {
        self.bitmap.append_bitmap(&other.bitmap);
        self.children_array
            .iter_mut()
            .enumerate()
            .try_for_each(|(i, a)| a.append_array(&other.children[i]))?;
        self.len += other.len();
        Ok(())
    }

    fn finish(mut self) -> Result<ListArray> {
        let children = self
            .children_array
            .into_iter()
            .map(|b| b.finish())
            .try_collect()?;
        Ok(ListArray {
            bitmap: self.bitmap.finish(),
            children,
            children_type: self.children_type,
            len: self.len,
        })
    }
}

#[derive(Debug)]
pub struct ListArray {
    bitmap: Bitmap,
    children: Vec<ArrayImpl>,
    children_type: Arc<[DataType]>,
    len: usize,
}

impl Array for ListArray {
    type RefItem<'a> = ListRef<'a>;

    type OwnedItem = ListValue;

    type Builder = ListArrayBuilder;

    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<ListRef<'_>> {
        if !self.is_null(idx) {
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
            array_type: ProstArrayType::List as i32,
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
        let array_builder = ListArrayBuilder::new_with_meta(
            capacity,
            ArrayMeta::List {
                children: self.children_type.clone(),
            },
        )?;
        Ok(ArrayBuilderImpl::List(array_builder))
    }

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::List {
            children: self.children_type.clone(),
        }
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
        let arr = ListArray {
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
    ) -> Result<ListArray> {
        let cardinality = null_bitmap.len();
        let bitmap = Bitmap::try_from(null_bitmap.to_vec())?;
        Ok(ListArray {
            bitmap,
            children_type: children_type.into(),
            len: cardinality,
            children,
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
    fields: Vec<Datum>,
}

impl fmt::Display for ListValue {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
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
    pub fn new(fields: Vec<Datum>) -> Self {
        Self { fields }
    }
}

#[derive(Copy, Clone)]
pub enum ListRef<'a> {
    Indexed { arr: &'a ListArray, idx: usize },
    ValueRef { val: &'a ListValue },
}

impl<'a> ListRef<'a> {
    pub fn fields_ref(&self) -> Vec<DatumRef<'a>> {
        match self {
            ListRef::Indexed { arr, idx } => {
                arr.children.iter().map(|a| a.value_at(*idx)).collect()
            }
            ListRef::ValueRef { val } => val
                .fields
                .iter()
                .map(|d| d.as_ref().map(|s| s.as_scalar_ref_impl()))
                .collect::<Vec<DatumRef<'a>>>(),
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
        self.fields_ref().eq(&other.fields_ref())
    }
}

impl PartialOrd for ListRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let l = self.fields_ref();
        let r = other.fields_ref();
        debug_assert_eq!(
            l.len(),
            r.len(),
            "Lists must have the same length to be compared"
        );
        let it = l.iter().enumerate().find_map(|(idx, v)| {
            let ord = cmp_list_field(v, &r[idx]);
            if let Ordering::Equal = ord {
                None
            } else {
                Some(ord)
            }
        });
        it.or(Some(Ordering::Equal))
    }
}

fn cmp_list_field(l: &Option<ScalarRefImpl>, r: &Option<ScalarRefImpl>) -> Ordering {
    match (l, r) {
        // Comparability check was performed by frontend beforehand.
        (Some(sl), Some(sr)) => sl.partial_cmp(sr).unwrap(),
        // Nulls are larger than everything, (1, null) > (1, 2) for example.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl Debug for ListRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ListRef::Indexed { arr, idx } => arr
                .children
                .iter()
                .try_for_each(|a| a.value_at(*idx).fmt(f)),
            ListRef::ValueRef { val } => write!(f, "{:?}", val),
        }
    }
}

impl Display for ListRef<'_> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
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
    use more_asserts::assert_gt;

    use super::*;
    use crate::{array, try_match_expand};

    // Empty list is allowed in postgres.
    // `CREATE TYPE foo_empty as ();`, e.g.
    #[test]
    fn test_list_new_empty() {
        let arr = ListArray::from_slices(&[true, false, true, false], vec![], vec![]).unwrap();
        let actual = ListArray::from_protobuf(&arr.to_protobuf().unwrap()).unwrap();
        assert_eq!(ArrayImpl::List(arr), actual);
    }

    #[test]
    fn test_list_with_fields() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[false, true, false, true],
            vec![
                array! { I32Array, [None, Some(1), None, Some(2)] }.into(),
                array! { F32Array, [None, Some(3.0), None, Some(4.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let actual = ListArray::from_protobuf(&arr.to_protobuf().unwrap()).unwrap();
        assert_eq!(ArrayImpl::List(arr), actual);

        let arr = try_match_expand!(actual, ArrayImpl::List).unwrap();
        let list_values = arr.values_vec();
        assert_eq!(
            list_values,
            vec![
                None,
                Some(ListValue::new(vec![
                    Some(ScalarImpl::Int32(1)),
                    Some(ScalarImpl::Float32(3.0.into())),
                ])),
                None,
                Some(ListValue::new(vec![
                    Some(ScalarImpl::Int32(2)),
                    Some(ScalarImpl::Float32(4.0.into())),
                ])),
            ]
        );

        let mut builder = ListArrayBuilder::new_with_meta(
            4,
            ArrayMeta::List {
                children: Arc::new([DataType::Int32, DataType::Float32]),
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
    }

    // Ensure `create_builder` exactly copies the same metadata.
    #[test]
    fn test_list_create_builder() {
        use crate::array::*;
        let arr = ListArray::from_slices(
            &[true],
            vec![
                array! { I32Array, [Some(1)] }.into(),
                array! { F32Array, [Some(2.0)] }.into(),
            ],
            vec![DataType::Int32, DataType::Float32],
        )
        .unwrap();
        let builder = arr.create_builder(4).unwrap();
        let arr2 = try_match_expand!(builder.finish().unwrap(), ArrayImpl::List).unwrap();
        assert_eq!(arr.array_meta(), arr2.array_meta());
    }

    #[test]
    fn test_list_value_cmp() {
        // (1, 2.0) > (1, 1.0)
        assert_gt!(
            ListValue::new(vec![Some(1.into()), Some(2.0.into())]),
            ListValue::new(vec![Some(1.into()), Some(1.0.into())]),
        );
        // null > 1
        assert_eq!(
            cmp_list_field(&None, &Some(ScalarRefImpl::Int32(1))),
            Ordering::Greater
        );
        // (1, null, 3) > (1, 1.0, 2)
        assert_gt!(
            ListValue::new(vec![Some(1.into()), None, Some(3.into())]),
            ListValue::new(vec![Some(1.into()), Some(1.0.into()), Some(2.into())]),
        );
        // (1, null) == (1, null)
        assert_eq!(
            ListValue::new(vec![Some(1.into()), None]),
            ListValue::new(vec![Some(1.into()), None]),
        );
    }
}
