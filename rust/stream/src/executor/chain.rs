use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::{Executor, Message, PkIndicesRef};
use crate::executor::ExecutorBuilder;
use crate::task::{ExecutorParams, StreamManagerCore};

#[derive(Debug)]
enum ChainState {
    Init,
    ReadingSnapshot,
    ReadingMView,
}

/// [`ChainExecutor`] is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, [`ChainExecutor`] is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
#[derive(Debug)]
pub struct ChainExecutor {
    snapshot: Box<dyn Executor>,
    mview: Box<dyn Executor>,
    state: ChainState,
    schema: Schema,
    column_idxs: Vec<usize>,
    /// Logical Operator Info
    op_info: String,
}

pub struct ChainExecutorBuilder {}

impl ExecutorBuilder for ChainExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::ChainNode)?;
        let snapshot = params.input.remove(1);
        let mview = params.input.remove(0);

        // TODO(MrCroxx): Use column_descs to get idx after mv planner can generate stable
        // column_ids. Now simply treat column_id as column_idx.
        // TODO(bugen): how can we know the way of mapping?
        let column_idxs: Vec<usize> = node.column_ids.iter().map(|id| *id as usize).collect();

        // The batch query executor scans on a mapped adhoc mview table, thus we should directly use
        // its schema.
        let schema = snapshot.schema().clone();
        Ok(Box::new(ChainExecutor::new(
            snapshot,
            mview,
            schema,
            column_idxs,
            params.op_info,
        )))
    }
}

impl ChainExecutor {
    pub fn new(
        snapshot: Box<dyn Executor>,
        mview: Box<dyn Executor>,
        schema: Schema,
        column_idxs: Vec<usize>,
        op_info: String,
    ) -> Self {
        Self {
            snapshot,
            mview,
            state: ChainState::Init,
            schema,
            column_idxs,
            op_info,
        }
    }

    fn mapping(&self, msg: Message) -> Result<Message> {
        match msg {
            Message::Chunk(chunk) => {
                let columns = self
                    .column_idxs
                    .iter()
                    .map(|i| chunk.columns()[*i].clone())
                    .collect();
                Ok(Message::Chunk(StreamChunk::new(
                    chunk.ops().to_vec(),
                    columns,
                    chunk.visibility().clone(),
                )))
            }
            _ => Ok(msg),
        }
    }

    /// Read next message from mview side.
    async fn read_mview(&mut self) -> Result<Message> {
        let msg = self.mview.next().await?;
        self.mapping(msg)
    }

    /// Read next message from snapshot side and update chain state if snapshot side reach EOF.
    async fn read_snapshot(&mut self) -> Result<Message> {
        let msg = self.snapshot.next().await;
        match msg {
            Err(e) => {
                // TODO: Refactor this once we find a better way to know the upstream is done.
                if let ErrorCode::Eof = e.inner() {
                    self.state = ChainState::ReadingMView;
                    return self.read_mview().await;
                }
                Err(e)
            }
            Ok(msg) => Ok(msg),
        }
    }

    async fn init(&mut self) -> Result<Message> {
        match self.read_mview().await? {
            // The first message should be a barrier with conf change from mview side.
            // Swallow it and get its barrier for snapshot read.
            Message::Chunk(_) => Err(ErrorCode::InternalError(
                "the first message received by chain node should be a barrier".to_owned(),
            )
            .into()),
            Message::Barrier(barrier) => {
                self.snapshot.init(barrier.epoch.prev)?;
                self.state = ChainState::ReadingSnapshot;
                Ok(Message::Barrier(barrier))
            }
        }
    }

    async fn next_inner(&mut self) -> Result<Message> {
        match &self.state {
            ChainState::Init => self.init().await,
            ChainState::ReadingSnapshot => self.read_snapshot().await,
            ChainState::ReadingMView => self.read_mview().await,
        }
    }
}

#[async_trait]
impl Executor for ChainExecutor {
    async fn next(&mut self) -> Result<Message> {
        self.next_inner().await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.mview.pk_indices()
    }

    fn identity(&self) -> &str {
        "Chain"
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }

    fn reset(&mut self, _epoch: u64) {
        // nothing to do
    }
}

#[cfg(test)]
mod test {

    use async_trait::async_trait;
    use risingwave_common::array::{Array, I32Array, Op, RwError, StreamChunk};
    use risingwave_common::catalog::Schema;
    use risingwave_common::column_nonnull;
    use risingwave_common::error::{ErrorCode, Result};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::plan::ColumnDesc;

    use super::ChainExecutor;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{Barrier, Executor, Message, PkIndices, PkIndicesRef};

    #[derive(Debug)]
    struct MockSnapshot(MockSource);

    impl MockSnapshot {
        pub fn with_chunks(
            schema: Schema,
            pk_indices: PkIndices,
            chunks: Vec<StreamChunk>,
        ) -> Self {
            Self(MockSource::with_chunks(schema, pk_indices, chunks))
        }

        async fn next_inner(&mut self) -> Result<Message> {
            match self.0.next().await {
                Ok(m) => {
                    if let Message::Barrier(_) = m {
                        // warning: translate all of the barrier types to the EOF here. May be an
                        // error in some circumstances.
                        Err(RwError::from(ErrorCode::Eof))
                    } else {
                        Ok(m)
                    }
                }
                Err(e) => Err(e),
            }
        }
    }

    #[async_trait]
    impl Executor for MockSnapshot {
        async fn next(&mut self) -> Result<Message> {
            self.next_inner().await
        }

        fn schema(&self) -> &Schema {
            self.0.schema()
        }

        fn pk_indices(&self) -> PkIndicesRef {
            self.0.pk_indices()
        }

        fn identity(&self) -> &'static str {
            "MockSnapshot"
        }

        fn logical_operator_info(&self) -> &str {
            self.identity()
        }

        fn init(&mut self, _: u64) -> Result<()> {
            Ok(())
        }

        fn reset(&mut self, _epoch: u64) {
            // nothing to do
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let columns = vec![ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            name: "v1".to_string(),
            ..Default::default()
        }];
        let schema = Schema::try_from(&columns).unwrap();
        let first = Box::new(MockSnapshot::with_chunks(
            schema.clone(),
            PkIndices::new(),
            vec![
                StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull! { I32Array, [1] }],
                    None,
                ),
                StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull! { I32Array, [2] }],
                    None,
                ),
            ],
        ));

        let second = Box::new(MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull! { I32Array, [3] }],
                    None,
                )),
                Message::Chunk(StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull! { I32Array, [4] }],
                    None,
                )),
            ],
        ));

        let mut chain =
            ChainExecutor::new(first, second, schema, vec![0], "ChainExecutor".to_string());
        let mut count = 0;
        loop {
            let k = &chain.next().await.unwrap();
            count += 1;
            if let Message::Chunk(ck) = k {
                let target = ck.column_at(0).array_ref().as_int32().value_at(0).unwrap();
                assert_eq!(target, count);
            } else {
                assert!(matches!(k, Message::Barrier(_)));
                return;
            }
        }
    }
}
