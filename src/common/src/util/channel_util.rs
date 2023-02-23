// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod broadcast {
    use tokio::sync::broadcast::error::RecvError;
    use tokio::sync::broadcast::Receiver;

    /// A wrapper around `[tokio::sync::broadcast::Receiver]` that instead of returning
    /// `Err(RecvError::Lagged)` when the capacity is reached, keep trying `recv` and ignoring
    /// skipped items until success.
    pub struct IgnoreLaggedReceiver<T> {
        inner: Receiver<T>,
    }

    impl<T: Clone> IgnoreLaggedReceiver<T> {
        pub async fn recv(&mut self) -> Result<T, RecvError> {
            loop {
                match self.inner.recv().await {
                    Ok(v) => break Ok(v),
                    Err(RecvError::Lagged(_)) => continue,
                    Err(e) => break Err(e),
                }
            }
        }
    }

    impl<T> From<Receiver<T>> for IgnoreLaggedReceiver<T> {
        fn from(inner: Receiver<T>) -> Self {
            Self { inner }
        }
    }
}

#[cfg(test)]
mod tests {
    mod broadcast {
        use tokio::sync::broadcast::channel;

        use crate::util::channel_util::broadcast::IgnoreLaggedReceiver;

        #[tokio::test]
        async fn test_ignore_lagged_receiver() {
            let (tx, rx) = channel(1);
            let mut rx = IgnoreLaggedReceiver::from(rx);
            for i in 0..2 {
                tx.send(i as u32).unwrap();
            }
            assert_eq!(rx.recv().await, Ok(1));
        }
    }
}
