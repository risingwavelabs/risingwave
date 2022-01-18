mod all_or_none;
pub use all_or_none::AllOrNoneState;
use risingwave_common::array::Row;
use risingwave_common::types::DataTypeKind;
use risingwave_storage::keyspace::Segment;
use risingwave_storage::{Keyspace, StateStore};

type ValueType = Row;
type PkType = Row;

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
