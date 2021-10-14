use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use super::{Message, Op, Result, StreamChunk};
use crate::array::column::Column;
use crate::array::{ArrayBuilder, ArrayImpl, I64ArrayBuilder};
use crate::stream_op::{StreamConsumer, StreamOperator};
use crate::types::Int64Type;
use async_trait::async_trait;

/// Mocked data has three columns:
/// linear and repeat are self defined iterators
/// and scalar is a constant.
pub struct MockData<I, J> {
    linear: I,
    scalar: i64,
    repeat: J,
}

impl<I, J> MockData<I, J> {
    pub fn new(linear: I, scalar: i64, repeat: J) -> Self {
        Self {
            linear,
            scalar,
            repeat,
        }
    }
}

impl<I: Iterator<Item = i64>, J: Iterator<Item = i64>> Iterator for MockData<I, J> {
    type Item = (i64, i64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(l) = self.linear.next() {
            let r = self.repeat.next().unwrap();
            Some((l, self.scalar, r))
        } else {
            None
        }
    }
}

pub struct MockDataSource<I: std::iter::Iterator<Item = i64>, J: std::iter::Iterator<Item = i64>> {
    inner: MockData<I, J>,
    is_running: bool,
}

impl<I, J> MockDataSource<I, J>
where
    I: std::iter::Iterator<Item = i64> + Send,
    J: std::iter::Iterator<Item = i64> + Send,
{
    pub fn new(inner: MockData<I, J>) -> Self {
        MockDataSource {
            inner,
            is_running: true,
        }
    }
}

impl<I, J> Debug for MockDataSource<I, J>
where
    I: std::iter::Iterator<Item = i64> + Send,
    J: std::iter::Iterator<Item = i64> + Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockDataSource").finish()
    }
}

#[async_trait]
impl<I, J> StreamOperator for MockDataSource<I, J>
where
    I: std::iter::Iterator<Item = i64> + Sync + Send + 'static,
    J: std::iter::Iterator<Item = i64> + Sync + Send + 'static,
{
    async fn next(&mut self) -> Result<Message> {
        const N: usize = 10;
        let mut count = 0;
        let mut col1 = I64ArrayBuilder::new(N)?;
        let mut col2 = I64ArrayBuilder::new(N)?;
        let mut col3 = I64ArrayBuilder::new(N)?;
        let mut ops = Vec::with_capacity(N);
        let op_cycle = vec![
            Op::Insert,
            Op::Delete,
            Op::UpdateDelete,
            Op::UpdateInsert,
            Op::Delete,
            Op::Insert,
            Op::UpdateDelete,
            Op::UpdateInsert,
        ];
        let mut total_input = 0;
        for _ in 0..N {
            match self.inner.next() {
                Some((i1, i2, i3)) => {
                    col1.append(Some(i1))?;
                    col2.append(Some(i2))?;
                    col3.append(Some(i3))?;
                    ops.push(op_cycle[total_input as usize % op_cycle.len()]);
                    total_input += 1;
                    count += 1;
                }
                None => break,
            }
        }
        if count == 0 {
            return Ok(Message::Terminate);
        }
        let col1 = Arc::new(ArrayImpl::Int64(col1.finish()?));
        let col2 = Arc::new(ArrayImpl::Int64(col2.finish()?));
        let col3 = Arc::new(ArrayImpl::Int64(col3.finish()?));
        let cols = vec![
            Column::new(col1, Int64Type::create(false)),
            Column::new(col2, Int64Type::create(false)),
            Column::new(col3, Int64Type::create(false)),
        ];
        let chunk = StreamChunk {
            visibility: None,
            ops,
            columns: cols,
        };
        return Ok(Message::Chunk(chunk));
    }
}

pub struct MockConsumer {
    input: Box<dyn StreamOperator>,
    data: Arc<Mutex<Vec<StreamChunk>>>,
}

impl MockConsumer {
    pub fn new(input: Box<dyn StreamOperator>, data: Arc<Mutex<Vec<StreamChunk>>>) -> Self {
        Self { input, data }
    }
}

#[async_trait]
impl StreamConsumer for MockConsumer {
    async fn next(&mut self) -> Result<bool> {
        match self.input.next().await? {
            Message::Chunk(chunk) => self.data.lock().unwrap().push(chunk),
            Message::Terminate => return Ok(false),
            _ => {} // do nothing
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Array;

    #[tokio::test]
    async fn test_data_source_read() -> Result<()> {
        let start: i64 = 114514;
        let end = start + 1000;
        let scalar: i64 = 0;
        let repeat: (i64, i64) = (-20, 20);
        let mock_data = MockData::new(start..end, scalar, (repeat.0..repeat.1).cycle());
        let source = MockDataSource::new(mock_data);
        let data = Arc::new(Mutex::new(vec![]));
        let mut consumer = MockConsumer::new(Box::new(source), data.clone());

        for _ in 0..10 {
            consumer.next().await?;
        }

        let data = data.lock().unwrap();
        let mut expected = start;
        for chunk in data.iter() {
            assert!(chunk.columns.len() == 3);
            let arr = chunk.columns[0].array_ref();
            if let ArrayImpl::Int64(arr) = arr {
                for i in 0..arr.len() {
                    let v = arr.value_at(i).expect("arr[i] exists");
                    assert_eq!(v, expected);
                    expected += 1;
                }
            } else {
                unreachable!()
            }
            let arr = chunk.columns[1].array_ref();
            if let ArrayImpl::Int64(arr) = arr {
                for i in 0..arr.len() {
                    let v = arr.value_at(i).expect("arr[i] exists");
                    assert_eq!(v, scalar);
                }
            } else {
                unreachable!()
            }
        }
        println!("{} items collected.", expected - start);
        Ok(())
    }
}
