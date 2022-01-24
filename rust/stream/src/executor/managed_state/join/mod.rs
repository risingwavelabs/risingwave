mod all_or_none;
use std::ops::Index;

pub use all_or_none::AllOrNoneState;
use risingwave_common::array::Row;
use risingwave_common::types::{deserialize_datum_from, serialize_datum_into, DataTypeKind, Datum};
use risingwave_storage::keyspace::Segment;
use risingwave_storage::{Keyspace, StateStore};
use serde::{Deserialize, Serialize};

/// This is a row with a match degree
#[derive(Clone)]
pub struct JoinRow {
    pub row: Row,
    degree: u64,
}

impl Index<usize> for JoinRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row[index]
    }
}

impl JoinRow {
    pub fn new(row: Row, degree: u64) -> Self {
        Self { row, degree }
    }

    pub fn size(&self) -> usize {
        self.row.size()
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    pub fn inc_degree(&mut self) -> u64 {
        self.degree += 1;
        self.degree
    }

    pub fn dec_degree(&mut self) -> u64 {
        self.degree -= 1;
        self.degree
    }

    /// Serialize the `JoinRow` into a memcomparable bytes. All values must not be null.
    pub fn serialize(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        for v in &self.row.0 {
            serialize_datum_into(v, &mut serializer)?;
        }
        self.degree.serialize(&mut serializer)?;
        Ok(serializer.into_inner())
    }
}

/// Deserializer of the `JoinRow`.
pub struct JoinRowDeserializer {
    data_type_kinds: Vec<DataTypeKind>,
}

impl JoinRowDeserializer {
    /// Creates a new `RowDeserializer` with row schema.
    pub fn new(schema: Vec<DataTypeKind>) -> Self {
        JoinRowDeserializer {
            data_type_kinds: schema,
        }
    }

    /// Deserialize the row from a memcomparable bytes.
    pub fn deserialize(&self, data: &[u8]) -> Result<JoinRow, memcomparable::Error> {
        let mut values = vec![];
        values.reserve(self.data_type_kinds.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for &ty in &self.data_type_kinds {
            values.push(deserialize_datum_from(&ty, &mut deserializer)?);
        }
        let degree = u64::deserialize(&mut deserializer)?;
        Ok(JoinRow {
            row: Row(values),
            degree,
        })
    }
}

type PkType = Row;

pub type StateValueType = JoinRow;

pub fn create_hash_join_state<S: StateStore>(
    key: PkType,
    keyspace: &Keyspace<S>,
    pk_indices: Vec<usize>,
    data_types: Vec<DataTypeKind>,
) -> AllOrNoneState<S> {
    // TODO: in pure in-memory engine, we should not do this serialization.
    let key_encoded = key.serialize().unwrap();

    let ks = keyspace.with_segment(Segment::VariantLength(key_encoded));

    AllOrNoneState::new(ks, data_types, pk_indices)
}
