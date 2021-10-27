use super::{Executor, Message, Result, SimpleExecutor, StreamChunk};
use crate::storage::MemRowTableRef as MemTableRef;
use crate::storage::Row;
use async_trait::async_trait;
use smallvec::SmallVec;

/// `MViewSinkExecutor` writes data to a row-based memtable, so that data could
/// be queried by the AP engine.
pub struct MViewSinkExecutor {
    input: Box<dyn Executor>,
    table: MemTableRef,
    pk_col: Vec<usize>,
}

impl MViewSinkExecutor {
    pub fn new(input: Box<dyn Executor>, table: MemTableRef, pk_col: Vec<usize>) -> Self {
        Self {
            input,
            table,
            pk_col,
        }
    }
}

impl SimpleExecutor for MViewSinkExecutor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns,
            visibility,
            ..
        } = &chunk;

        let mut ingest_op = vec![];
        let mut row_batch = vec![];

        let pk_num = &self.pk_col.len();

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
                let datum = columns[*column_id].array_ref().datum_at(idx);
                pk_row.push(datum);
            }
            let pk_row = Row(pk_row);

            // assemble row
            let mut row = SmallVec::new();
            for column in columns {
                let datum = column.array_ref().datum_at(idx);
                row.push(datum);
            }
            let row = Row(row);

            use super::Op::*;
            if *pk_num > 0 {
                match op {
                    Insert | UpdateInsert => {
                        ingest_op.push((pk_row, Some(row)));
                    }
                    Delete | UpdateDelete => {
                        ingest_op.push((pk_row, None));
                    }
                }
            } else {
                match op {
                    Insert | UpdateInsert => {
                        row_batch.push((row, true));
                    }
                    Delete | UpdateDelete => {
                        row_batch.push((row, false));
                    }
                }
            }
        }
        if *pk_num > 0 {
            self.table.ingest(ingest_op)?;
        } else {
            self.table.insert_batch(row_batch)?;
        }

        Ok(Message::Chunk(chunk))
    }
}

use crate::impl_consume_barrier_default;

impl_consume_barrier_default!(MViewSinkExecutor, Executor);
