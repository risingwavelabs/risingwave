use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::{Message, Op, Output, Result, StreamChunk};
use crate::array2::column::Column;
use crate::array2::{ArrayBuilder, ArrayImpl, I64ArrayBuilder};
use crate::types::Int64Type;
use async_trait::async_trait;
use futures::channel::oneshot;

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

/// `DataSource` is the source of streaming. A `DataSource` runs in background inside
/// a `SourceProcessor`, generating chunks continuously unless `cancel` signal received.
#[async_trait]
pub trait DataSource: Debug + Send + Sync + 'static {
    async fn run(&mut self, output: Box<dyn Output>, cancel: oneshot::Receiver<()>) -> Result<()>;
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
impl<I, J> DataSource for MockDataSource<I, J>
where
    I: std::iter::Iterator<Item = i64> + Sync + Send + 'static,
    J: std::iter::Iterator<Item = i64> + Sync + Send + 'static,
{
    async fn run(
        &mut self,
        mut output: Box<dyn Output>,
        mut cancel: oneshot::Receiver<()>,
    ) -> Result<()> {
        const N: usize = 10;
        loop {
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
                if cancel.try_recv().expect("sender dropped").is_some() {
                    break;
                }
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
                break;
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
            output.collect(Message::Chunk(chunk)).await?;
        }

        Ok(())
    }
}

pub struct MockOutput {
    data: Arc<Mutex<Vec<StreamChunk>>>,
}

impl MockOutput {
    pub fn new(data: Arc<Mutex<Vec<StreamChunk>>>) -> Self {
        Self { data }
    }
}

#[async_trait]
impl Output for MockOutput {
    async fn collect(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Chunk(chunk) => self.data.lock().await.push(chunk),
            _ => unreachable!(),
        }
        Ok(())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use crate::array2::Array;

    #[tokio::test]
    async fn test_data_source_read() -> Result<()> {
        let start: i64 = 114514;
        let end = start + 1000;
        let scalar: i64 = 0;
        let repeat: (i64, i64) = (-20, 20);
        let mock_data = MockData::new(start..end, scalar, (repeat.0..repeat.1).cycle());
        let mut source = MockDataSource::new(mock_data);
        let data = Arc::new(Mutex::new(vec![]));
        let output = MockOutput { data: data.clone() };

        let output = Box::new(output);
        let (_cancel_tx, cancel_rx) = oneshot::channel();
        source.run(output, cancel_rx).await?;

        let data = data.lock().await;
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
