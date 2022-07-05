// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use tokio::sync::{mpsc, Mutex};

use super::cache::FlushBufferHook;
use super::coding::CacheKey;
use super::error::Result;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct TestCacheKey(pub u64);

impl CacheKey for TestCacheKey {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
    }

    fn decode(mut buf: &[u8]) -> Self {
        Self(buf.get_u64())
    }
}

pub fn key(v: u64) -> TestCacheKey {
    TestCacheKey(v)
}

#[derive(Clone)]
pub struct FlushHolder {
    pre_sender: mpsc::UnboundedSender<()>,
    pre_receiver: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,

    post_sender: mpsc::UnboundedSender<()>,
    post_receiver: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,
}

impl FlushHolder {
    pub fn new() -> Self {
        let (tx0, rx0) = mpsc::unbounded_channel();
        let (tx1, rx1) = mpsc::unbounded_channel();
        Self {
            pre_sender: tx0,
            pre_receiver: Arc::new(Mutex::new(rx0)),

            post_sender: tx1,
            post_receiver: Arc::new(Mutex::new(rx1)),
        }
    }

    pub fn trigger(&self) {
        self.pre_sender.send(()).unwrap();
    }

    pub async fn wait(&self) {
        self.post_receiver.lock().await.recv().await.unwrap();
    }
}

#[async_trait]
impl FlushBufferHook for FlushHolder {
    async fn pre_flush(&self) -> Result<()> {
        self.pre_receiver.lock().await.recv().await.unwrap();
        Ok(())
    }

    async fn post_flush(&self) -> Result<()> {
        self.post_sender.send(()).unwrap();
        Ok(())
    }
}
