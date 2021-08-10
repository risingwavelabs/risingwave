#[derive(Copy, Clone, Debug, Hash, PartialOrd, PartialEq)]
pub(crate) struct DatabaseId {
    id: i32,
}

#[derive(Copy, Clone, Debug, Hash, PartialOrd, PartialEq)]
pub(crate) struct SchemaId {
    db_id: DatabaseId,
    id: i32,
}

#[derive(Copy, Clone, Debug, Hash, PartialOrd, PartialEq)]
pub(crate) struct TableId {
    schema_id: SchemaId,
    id: i32,
}
