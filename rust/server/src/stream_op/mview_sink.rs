use super::{Message, Result, SimpleStreamOperator, StreamChunk, StreamOperator};
use crate::storage::MemRowTableRef as MemTableRef;
use crate::storage::Row;
use async_trait::async_trait;
use smallvec::SmallVec;

/// `MViewSinkExecutor` writes data to a row-based memtable, so that data could
/// be queried by the AP engine.
pub struct MViewSinkExecutor {
    input: Box<dyn StreamOperator>,
    table: MemTableRef,
    pk_col: Vec<usize>,
}

impl MViewSinkExecutor {
    pub fn new(input: Box<dyn StreamOperator>, table: MemTableRef, pk_col: Vec<usize>) -> Self {
        Self {
            input,
            table,
            pk_col,
        }
    }
}

impl SimpleStreamOperator for MViewSinkExecutor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns,
            visibility,
            ..
        } = &chunk;

        let mut ingest_op = vec![];

        for (idx, op) in ops.iter().enumerate() {
            // check visibility
            let visible = visibility
                .as_ref()
                .map(|x| x.is_set(idx).unwrap())
                .unwrap_or(true);
            if !visible {
                continue;
            }

            // assemble pk row
            let mut pk_row = SmallVec::new();
            for column_id in &self.pk_col {
                let scalar_ref = columns[*column_id].array_ref().scalar_value_at(idx);
                pk_row.push(scalar_ref);
            }
            let pk_row = Row(pk_row);

            // assemble row
            let mut row = SmallVec::new();
            for column in columns {
                let scalar_ref = column.array_ref().scalar_value_at(idx);
                row.push(scalar_ref);
            }
            let row = Row(row);

            use super::Op::*;
            match op {
                Insert | UpdateInsert => {
                    ingest_op.push((pk_row, Some(row)));
                }
                Delete | UpdateDelete => {
                    ingest_op.push((pk_row, None));
                }
            }
        }

        self.table.ingest(ingest_op)?;

        Ok(Message::Chunk(chunk))
    }
}

use crate::impl_consume_barrier_default;

impl_consume_barrier_default!(MViewSinkExecutor, StreamOperator);
