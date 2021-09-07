use super::{Output, Result};

pub trait DataSource: Sync {
    fn run(&self, output: Box<dyn Output>) -> Result<()>;
    fn cancel(&self) -> Result<()>;
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::time;

    use super::super::{Op, Output, Result, StreamChunk};
    use super::DataSource;
    use crate::array2::{Array, ArrayBuilder, ArrayImpl, PrimitiveArrayBuilder};

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

    impl<I> DataSource for MockDataSource<I>
    where
        I: std::iter::Iterator<Item = i64> + Send,
    {
        fn run(&self, mut output: Box<dyn Output>) -> Result<()> {
            const N: usize = 10;
            loop {
                let mut core = self.core.lock().unwrap();
                if !core.is_running {
                    break;
                }
                let mut col1 = PrimitiveArrayBuilder::<i64>::new(N);
                for _ in 0..N {
                    match core.inner.next() {
                        Some(i) => {
                            col1.append(Some(i));
                        }
                        None => break,
                    }
                }
                let col1 = ArrayImpl::Int64(col1.finish());
                let cols = vec![col1];
                let chunk = StreamChunk {
                    cardinality: N,
                    visibility: None,
                    ops: vec![Op::Insert; N],
                    arrays: cols,
                };
                output.collect(chunk)?;
            }

            Ok(())
        }

        fn cancel(&self) -> Result<()> {
            let mut core = self.core.lock().unwrap();
            core.is_running = false;

            Ok(())
        }
    }

    struct MockOutput {
        data: Arc<Mutex<Vec<StreamChunk>>>,
    }

    impl Output for MockOutput {
        fn collect(&mut self, chunk: StreamChunk) -> Result<()> {
            self.data.lock().unwrap().push(chunk);
            Ok(())
        }
    }

    #[test]
    fn test_read() -> Result<()> {
        let start: i64 = 114514;
        let source = Arc::new(MockDataSource::new(start..));
        let data = Arc::new(Mutex::new(vec![]));
        let output = MockOutput { data: data.clone() };
        let source2 = source.clone();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(time::Duration::from_millis(10));
            source.cancel().expect("cancel without error");
        });

        let output = Box::new(output);
        source2.run(output).expect("run without error");

        handle.join().unwrap();

        let data = data.lock().unwrap();
        let mut expected = start;
        for chunk in data.iter() {
            assert!(chunk.arrays.len() == 1);
            let arr = &chunk.arrays[0];
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
