use crate::array::DataChunk;
use crate::error::Result;
use crate::executor::hash_map::HashKey;
use crate::executor::join::hash_join::HashJoinState::{FirstProbe, Probe, ProbeRemaining};
use crate::executor::join::hash_join_state::{BuildTable, ProbeTable};
use crate::executor::join::JoinType;
use crate::executor::ExecutorResult::Batch;
use crate::executor::{BoxedExecutor, Executor, ExecutorResult};
use crate::types::DataTypeRef;
use either::Either;
use std::convert::TryInto;
use std::mem::swap;

/// Parameters of equi-join.
/// We use following sql as an example in comments:
/// ```sql
/// select a.a1, a.a2, b.b1, b.b2 from a inner join b where a.a3 = b.b3 and a.a1 = b.b1
/// ```
#[derive(Clone, Default)]
pub(super) struct EquiJoinParams {
    join_type: JoinType,
    /// Column indexes of left keys in equi join, e.g., the column indexes of `b1` and `b3` in `b`.
    left_key_columns: Vec<usize>,
    /// Data types of left keys in equi join, e.g., the column types of `b1` and `b3` in `b`.
    left_key_types: Vec<DataTypeRef>,
    /// Column indexes of right keys in equi join, e.g., the column indexes of `a1` and `a3` in `a`.
    right_key_columns: Vec<usize>,
    /// Data types of right keys in equi join, e.g., the column types of `a1` and `a3` in `a`.
    right_key_types: Vec<DataTypeRef>,
    /// Column indexes of outputs in equi join, e.g. the column indexes of `a1`, `a2`, `b1`, `b2`.
    /// [`Either::Left`] is used to mark left side input, and [`Either::Right`] is used to mark right
    /// side input.
    output_columns: Vec<Either<usize, usize>>,
    /// Column types of outputs in equi join, e.g. the column types of `a1`, `a2`, `b1`, `b2`.
    output_data_types: Vec<DataTypeRef>,
}

/// Different states when executing a hash join.
enum HashJoinState<K> {
    /// Initial state of hash join.
    /// In this state, the executor [`Executor::init`] build side input, and calls [`Executor::next`]
    /// of build side input till [`ExecutorResult::Done`] is returned to create `BuildTable`.
    Build(BuildTable),
    /// First state after finishing build state.
    /// It's different from [`Probe`] in that we need to [`Executor::init`] probe side input.
    FirstProbe(ProbeTable<K>),
    /// State for executing join.
    /// In this state, the executor calls [`Executor::init`]  method of probe side input, and executes
    /// joining with the chunk against build table to create output.
    Probe(ProbeTable<K>),
    /// Optional state after [`Probe`].
    /// This state is only required for join types which may produce outputs from build side, for
    /// example, [`JoinType::RightOuter`].
    ///
    /// See [`JoinType::need_join_remaining()`]
    ProbeRemaining(ProbeTable<K>),
    /// Final state of hash join.
    Done,
}

pub(super) struct HashJoinExecutor<K> {
    /// Probe side
    left_child: BoxedExecutor,
    /// Build side
    right_child: BoxedExecutor,
    state: HashJoinState<K>,
}

impl EquiJoinParams {
    #[inline(always)]
    pub(super) fn probe_key_columns(&self) -> &[usize] {
        &self.left_key_columns
    }

    #[inline(always)]
    pub(super) fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[inline(always)]
    pub(super) fn output_types(&self) -> &[DataTypeRef] {
        &self.output_data_types
    }

    #[inline(always)]
    pub(super) fn output_columns(&self) -> &[Either<usize, usize>] {
        &self.output_columns
    }

    #[inline(always)]
    pub(super) fn build_key_columns(&self) -> &[usize] {
        &self.right_key_columns
    }
}

impl<K: HashKey + Send + Sync> Executor for HashJoinExecutor<K> {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        loop {
            let mut cur_state = HashJoinState::Done;
            swap(&mut cur_state, &mut self.state);

            match cur_state {
                HashJoinState::Build(build_table) => self.build(build_table)?,
                HashJoinState::FirstProbe(probe_table) => {
                    let ret = self.probe(true, probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                HashJoinState::Probe(probe_table) => {
                    let ret = self.probe(false, probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                HashJoinState::ProbeRemaining(probe_table) => {
                    let ret = self.probe_remaining(probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                HashJoinState::Done => return Ok(ExecutorResult::Done),
            }
        }
    }

    fn clean(&mut self) -> Result<()> {
        self.left_child.clean()?;
        self.right_child.clean()?;
        Ok(())
    }
}

impl<K: HashKey> HashJoinExecutor<K> {
    fn build(&mut self, mut build_table: BuildTable) -> Result<()> {
        self.right_child.init()?;
        while let Batch(chunk) = self.right_child.execute()? {
            build_table.append_build_chunk(chunk)?;
        }

        let probe_table = build_table.try_into()?;

        self.state = FirstProbe(probe_table);
        Ok(())
    }

    fn probe(
        &mut self,
        first_probe: bool,
        mut probe_table: ProbeTable<K>,
    ) -> Result<Option<DataChunk>> {
        if first_probe {
            self.left_child.init()?;
        }

        match self.left_child.execute()? {
            ExecutorResult::Batch(data_chunk) => {
                let ret = probe_table.join(data_chunk).map(Some);
                self.state = Probe(probe_table);
                ret
            }
            ExecutorResult::Done => {
                if probe_table.join_type().need_join_remaining() {
                    // switch state
                    self.state = ProbeRemaining(probe_table);
                } else {
                    self.state = HashJoinState::Done;
                }

                Ok(None)
            }
        }
    }

    fn probe_remaining(&mut self, mut probe_table: ProbeTable<K>) -> Result<Option<DataChunk>> {
        let ret = probe_table.join_remaining()?;
        self.state = HashJoinState::Done;
        Ok(ret)
    }
}

impl<K> HashJoinExecutor<K> {
    fn new(left_child: BoxedExecutor, right_child: BoxedExecutor, params: EquiJoinParams) -> Self {
        HashJoinExecutor {
            left_child,
            right_child,
            state: HashJoinState::Build(BuildTable::with_params(params)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::array;
    use crate::array::column::Column;
    use crate::array::{ArrayBuilderImpl, DataChunk};
    use crate::array::{F32Array, F64Array, I32Array};
    use crate::error::Result;
    use crate::executor::hash_map::Key32;
    use crate::executor::join::hash_join::{EquiJoinParams, HashJoinExecutor};
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{BoxedExecutor, ExecutorResult};
    use crate::types::{DataTypeRef, Float32Type, Float64Type, Int32Type};
    use either::Either;
    use std::sync::Arc;

    struct DataChunkMerger {
        data_types: Vec<DataTypeRef>,
        array_builders: Vec<ArrayBuilderImpl>,
    }

    impl DataChunkMerger {
        fn new(data_types: Vec<DataTypeRef>) -> Result<Self> {
            let array_builders = data_types
                .clone()
                .into_iter()
                .map(|data_type| data_type.create_array_builder(1024))
                .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

            Ok(Self {
                data_types,
                array_builders,
            })
        }

        fn append(&mut self, data_chunk: &DataChunk) -> Result<()> {
            ensure!(self.array_builders.len() == data_chunk.dimension());
            for idx in 0..self.array_builders.len() {
                self.array_builders[idx].append_array(data_chunk.column_at(idx)?.array_ref())?;
            }

            Ok(())
        }

        fn finish(self) -> Result<DataChunk> {
            let columns = self
                .data_types
                .iter()
                .zip(self.array_builders)
                .map(|(data_type, array_builder)| {
                    array_builder
                        .finish()
                        .map(|arr| Column::new(Arc::new(arr), data_type.clone()))
                })
                .collect::<Result<Vec<Column>>>()?;

            DataChunk::try_from(columns)
        }
    }

    impl PartialEq for DataChunk {
        fn eq(&self, other: &Self) -> bool {
            assert!(self.visibility().is_none());
            assert!(other.visibility().is_none());

            if self.cardinality() != other.cardinality() {
                return false;
            }

            self.iter()
                .zip(other.iter())
                .all(|(row1, row2)| row1 == row2)
        }
    }

    struct TestFixture {
        left_types: Vec<DataTypeRef>,
        right_types: Vec<DataTypeRef>,
        join_type: JoinType,
    }

    /// Sql for creating test data:
    /// ```sql
    /// drop table t1 if exists;
    /// create table t1(v1 int, v2 float);
    /// insert into t1 values
    /// (1, 6.1::FLOAT), (2, null), (null, 8.4::FLOAT), (3, 3.9::FLOAT), (null, null),
    /// (4, 6.6::FLOAT), (3, null), (null, 0.7::FLOAT), (5, null), (null, 5.5::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (8, 6.1::REAL), (2, null), (null, 8.9::REAL), (3, null), (null, 3.5::REAL),
    /// (6, null), (4, 7.5::REAL), (6, null), (null, 8::REAL), (7, null),
    /// (null, 9.1::REAL), (9, null), (3, 5.7::REAL), (9, null), (null, 9.6::REAL),
    /// (100, null), (null, 8.18::REAL), (200, null);
    ///```
    impl TestFixture {
        fn with_join_type(join_type: JoinType) -> Self {
            Self {
                left_types: vec![
                    Arc::new(Int32Type::new(true)),
                    Arc::new(Float32Type::new(true)),
                ],
                right_types: vec![
                    Arc::new(Int32Type::new(true)),
                    Arc::new(Float64Type::new(true)),
                ],
                join_type,
            }
        }
        fn create_left_executor(&self) -> BoxedExecutor {
            let mut executor = MockExecutor::new();

            {
                let column1 = Column::new(
                    Arc::new(array! {I32Array, [Some(1), Some(2), None, Some(3), None]}.into()),
                    self.left_types[0].clone(),
                );
                let column2 = Column::new(
                    Arc::new(
                        array! {F32Array, [Some(6.1f32), None, Some(8.4f32), Some(3.9f32), None]}
                            .into(),
                    ),
                    self.left_types[1].clone(),
                );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(
                    Arc::new(array! {I32Array, [Some(4), Some(3), None, Some(5), None]}.into()),
                    self.left_types[0].clone(),
                );
                let column2 = Column::new(
                    Arc::new(
                        array! {F32Array, [Some(6.6f32), None, Some(0.7f32), None, Some(5.5f32)]}
                            .into(),
                    ),
                    self.left_types[1].clone(),
                );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let mut executor = MockExecutor::new();

            {
                let column1 = Column::new(
                    Arc::new(
                        array! {I32Array, [Some(8), Some(2), None, Some(3), None, Some(6)]}.into(),
                    ),
                    self.right_types[0].clone(),
                );

                let column2 = Column::new(
          Arc::new(
            array! {F64Array, [Some(6.1f64), None, Some(8.9f64), None, Some(3.5f64), None]}.into(),
          ),
          self.right_types[1].clone(),
        );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(
                    Arc::new(
                        array! {I32Array, [Some(4), Some(6), None, Some(7), None, Some(9)]}.into(),
                    ),
                    self.right_types[0].clone(),
                );

                let column2 = Column::new(
          Arc::new(
            array! {F64Array, [Some(7.5f64), None, Some(8f64), None, Some(9.1f64), None]}.into(),
          ),
          self.right_types[1].clone(),
        );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(
                    Arc::new(
                        array! {I32Array, [Some(3), Some(9), None, Some(100), None, Some(200)]}
                            .into(),
                    ),
                    self.right_types[0].clone(),
                );

                let column2 = Column::new(
          Arc::new(
            array! {F64Array, [Some(5.7f64), None, Some(9.6f64), None, Some(8.18f64), None]}.into(),
          ),
          self.right_types[1].clone(),
        );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            Box::new(executor)
        }

        fn output_columns(&self) -> Vec<Either<usize, usize>> {
            match self.join_type {
                JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => {
                    vec![Either::Left(1), Either::Right(1)]
                }
                JoinType::LeftAnti | JoinType::LeftSemi => vec![Either::Left(1)],
                JoinType::RightAnti | JoinType::RightSemi => vec![Either::Right(1)],
            }
        }

        fn output_data_types(&self) -> Vec<DataTypeRef> {
            let output_columns = self.output_columns();

            output_columns
                .iter()
                .map(|column| match column {
                    Either::Left(idx) => self.left_types[*idx].clone(),
                    Either::Right(idx) => self.right_types[*idx].clone(),
                })
                .collect::<Vec<DataTypeRef>>()
        }

        fn create_join_executor(&self) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let output_columns = self.output_columns();

            let output_data_types = self.output_data_types();

            let params = EquiJoinParams {
                join_type,
                left_key_columns: vec![0],
                left_key_types: vec![self.left_types[0].clone()],
                right_key_columns: vec![0],
                right_key_types: vec![self.right_types[0].clone()],
                output_columns,
                output_data_types,
            };

            Box::new(HashJoinExecutor::<Key32>::new(
                left_child,
                right_child,
                params,
            )) as BoxedExecutor
        }

        fn do_test(&self, expected: DataChunk) {
            let mut join_executor = self.create_join_executor();
            join_executor.init().expect("Failed to init join executor.");

            let mut data_chunk_merger = DataChunkMerger::new(self.output_data_types()).unwrap();

            while let ExecutorResult::Batch(data_chunk) = join_executor.execute().unwrap() {
                data_chunk_merger.append(&data_chunk).unwrap();
            }

            let result_chunk = data_chunk_merger.finish().unwrap();
            // TODO: Replace this with unsorted comparison
            assert_eq!(expected, result_chunk);
        }
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 join t2 on t1.v1 = t2.v1;
    /// ```
    #[test]
    fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let column1 = Column::new(
            Arc::new(
                array! {F32Array, [None, Some(3.9f32), Some(3.9f32), Some(6.6f32), None, None]}
                    .into(),
            ),
            test_fixture.left_types[1].clone(),
        );

        let column2 = Column::new(
            Arc::new(
                array! {F64Array, [None, Some(5.7f64), None,  Some(7.5f64), Some(5.7f64),  None]}
                    .into(),
            ),
            test_fixture.right_types[1].clone(),
        );

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[test]
    fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let column1 = Column::new(
      Arc::new(
        array! {F32Array, [Some(6.1f32), None, Some(8.4f32), Some(3.9f32), Some(3.9f32), None,
        Some(6.6f32), None, None, Some(0.7f32), None, Some(5.5f32)]}
        .into(),
      ),
      test_fixture.left_types[1].clone(),
    );

        let column2 = Column::new(
      Arc::new(
        array! {F64Array, [None, None, None, Some(5.7f64), None, None, Some(7.5f64), Some(5.7f64),
        None, None, None, None]}
        .into(),
      ),
      test_fixture.right_types[1].clone(),
    );

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 right outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[test]
    fn test_right_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let column1 = Column::new(
            Arc::new(
                array! {F32Array, [
                  None, Some(3.9f32), Some(3.9f32), Some(6.6), None,
                  None, None, None, None, None,
                  None, None, None, None, None,
                  None, None, None, None, None
                ]}
                .into(),
            ),
            test_fixture.left_types[1].clone(),
        );

        let column2 = Column::new(
            Arc::new(
                array! {F64Array, [
                None, Some(5.7f64), None, Some(7.5f64), Some(5.7f64),
                None, Some(6.1f64), Some(8.9f64), Some(3.5f64), None,
                None, Some(8.0f64), None, Some(9.1f64), None,
                None, Some(9.6f64),None, Some(8.18f64), None]}
                .into(),
            ),
            test_fixture.right_types[1].clone(),
        );

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    ///```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 full outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[test]
    fn test_full_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let column1 = Column::new(
            Arc::new(
                array! {F32Array, [
                  Some(6.1f32), None, Some(8.4f32), Some(3.9f32), Some(3.9f32),
                  None, Some(6.6f32), None, None, Some(0.7f32),
                  None, Some(5.5f32), None, None, None,
                  None, None, None, None, None,
                  None, None, None, None, None,
                  None
                ]}
                .into(),
            ),
            test_fixture.left_types[1].clone(),
        );

        let column2 = Column::new(
            Arc::new(
                array! {F64Array, [
                  None, None, None, Some(5.7f64), None,
                  None, Some(7.5f64), Some(5.7f64), None, None,
                  None, None, Some(6.1f64), Some(8.9f64), Some(3.5f64),
                  None, None, Some(8.0f64), None, Some(9.1f64),
                  None, None, Some(9.6f64), None, Some(8.18f64),
                  None
                ]}
                .into(),
            ),
            test_fixture.right_types[1].clone(),
        );

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    #[test]
    fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let column1 = Column::new(
            Arc::new(
                array! {F32Array, [
                  Some(6.1f32), Some(8.4f32), None, Some(0.7f32), None, Some(5.5f32)
                ]}
                .into(),
            ),
            test_fixture.left_types[1].clone(),
        );

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    #[test]
    fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let column1 = Column::new(
            Arc::new(
                array! {F32Array, [
                  None, Some(3.9f32), Some(6.6f32), None
                ]}
                .into(),
            ),
            test_fixture.left_types[1].clone(),
        );

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    #[test]
    fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let column1 = Column::new(
            Arc::new(
                array! {F64Array, [
                  Some(6.1f64), Some(8.9f64), Some(3.5f64), None, None,
                  Some(8.0f64), None, Some(9.1f64), None, None,
                  Some(9.6f64), None, Some(8.18f64), None
                ]}
                .into(),
            ),
            test_fixture.right_types[1].clone(),
        );

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }

    #[test]
    fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let column1 = Column::new(
            Arc::new(
                array! {F64Array, [
                  None, Some(5.7f64), None, Some(7.5f64)
                ]}
                .into(),
            ),
            test_fixture.right_types[1].clone(),
        );

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk);
    }
}
