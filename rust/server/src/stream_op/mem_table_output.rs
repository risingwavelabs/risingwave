use super::{Message, Output, Result, StreamChunk};
use crate::storage::MemRowTableRef as MemTableRef;
use crate::storage::Row;
use async_trait::async_trait;
use smallvec::SmallVec;

/// `MemTableOutput` writes data to a row-based memtable, so that data could
/// be quried by the AP engine.
pub struct MemTableOutput {
    table: MemTableRef,
    pk_col: Vec<usize>,
}

impl MemTableOutput {
    pub fn new(table: MemTableRef, pk_col: Vec<usize>) -> Self {
        Self { table, pk_col }
    }
}

#[async_trait]
impl Output for MemTableOutput {
    async fn collect(&mut self, msg: Message) -> Result<()> {
        if let Message::Chunk(chunk) = msg {
            let StreamChunk {
                ops,
                columns,
                visibility,
                ..
            } = chunk;

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
                    let scalar_ref = columns[*column_id].array_ref().value_at_owned(idx);
                    pk_row.push(scalar_ref);
                }
                let pk_row = Row(pk_row);

                // assemble row
                let mut row = SmallVec::new();
                for column in &columns {
                    let scalar_ref = column.array_ref().value_at_owned(idx);
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
        }
        Ok(())
    }
}
