use super::{Output, Result};

use async_trait::async_trait;

#[async_trait]
pub trait DataSource: Send + Sync + 'static {
    async fn run(&self, output: Box<dyn Output>) -> Result<()>;
    async fn cancel(&self) -> Result<()>;
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time;
    use tokio::sync::Mutex;

    use super::super::{Message, Op, Output, Result, StreamChunk};
    use super::*;
    use crate::array2::column::Column;
    use crate::array2::{Array, ArrayBuilder, ArrayImpl, I64ArrayBuilder};
    use crate::types::Int64Type;
    struct MockDataSourceCore<I: std::iter::Iterator<Item = i64>> {
        inner: I,
        is_running: bool,
    }

    struct MockDataSource<I: std::iter::Iterator<Item = i64>> {
        core: Mutex<MockDataSourceCore<I>>,
    }

    impl<I> MockDataSource<I>
    where
        I: std::iter::Iterator<Item = i64> + Send,
    {
        fn new(inner: I) -> Self {
            let core = MockDataSourceCore {
                inner,
                is_running: true,
            };
            MockDataSource {
                core: Mutex::new(core),
            }
        }
    }

    #[async_trait]
    impl<I> DataSource for MockDataSource<I>
    where
        I: std::iter::Iterator<Item = i64> + Sync + Send + 'static,
    {
        async fn run(&self, mut output: Box<dyn Output>) -> Result<()> {
            const N: usize = 10;
            loop {
                let mut core = self.core.lock().await;
                if !core.is_running {
                    break;
                }
                let mut col1 = I64ArrayBuilder::new(N)?;
                for _ in 0..N {
                    match core.inner.next() {
                        Some(i) => {
                            col1.append(Some(i))?;
                        }
                        None => break,
                    }
                }
                let col1 = Arc::new(ArrayImpl::Int64(col1.finish()?));
                let cols = vec![Column::new(col1, Arc::new(Int64Type::new(false)))];
                let chunk = StreamChunk {
                    cardinality: N,
                    visibility: None,
                    ops: vec![Op::Insert; N],
                    columns: cols,
                };
                output.collect(Message::Chunk(chunk)).await?;
            }

            Ok(())
        }

        async fn cancel(&self) -> Result<()> {
            let mut core = self.core.lock().await;
            core.is_running = false;

            Ok(())
        }
    }

    struct MockOutput {
        data: Arc<Mutex<Vec<StreamChunk>>>,
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

    #[tokio::test]
    async fn test_read() -> Result<()> {
        let start: i64 = 114514;
        let source = Arc::new(MockDataSource::new(start..));
        let data = Arc::new(Mutex::new(vec![]));
        let output = MockOutput { data: data.clone() };
        let source2 = source.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(time::Duration::from_millis(10)).await;
            source.cancel().await.expect("cancel without error");
        });

        let output = Box::new(output);
        source2.run(output).await.expect("run without error");

        handle.await.unwrap();

        let data = data.lock().await;
        let mut expected = start;
        for chunk in data.iter() {
            assert!(chunk.columns.len() == 1);
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
        }
        println!("{} items collected.", expected - start);
        Ok(())
    }
}
