// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use opendal::layers::{
    ConcurrentLimitLayer as OpendalConcurrentLimitLayer, ConcurrentLimitSemaphore,
};
use opendal::raw::{
    Layer, OpCopier, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceInfo, Servicer, oio,
};
use opendal::{Capability, OperationContext, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
struct SharedSemaphore(Arc<Semaphore>);

impl SharedSemaphore {
    fn new(permits: usize) -> Self {
        Self(Arc::new(Semaphore::new(permits)))
    }
}

impl ConcurrentLimitSemaphore for SharedSemaphore {
    type Permit = OwnedSemaphorePermit;

    async fn acquire(&self) -> Self::Permit {
        self.0
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed")
    }
}

/// `OpenDAL`'s concurrency limiter, with list permits scoped to each `next` call.
/// Keeping a lister alive between GC batches must not prevent stat or delete from
/// acquiring the same operation permit. Other operations and HTTP bodies retain
/// `OpenDAL`'s existing permit lifetimes.
#[derive(Clone, Debug)]
pub(super) struct ConcurrentLimitLayer {
    inner: OpendalConcurrentLimitLayer<SharedSemaphore>,
    semaphore: SharedSemaphore,
}

impl ConcurrentLimitLayer {
    pub(super) fn new(permits: usize) -> Self {
        let semaphore = SharedSemaphore::new(permits);
        Self {
            inner: OpendalConcurrentLimitLayer::with_semaphore(semaphore.clone()),
            semaphore,
        }
    }

    pub(super) fn with_http_concurrent_limit(mut self, permits: usize) -> Self {
        self.inner = self
            .inner
            .with_http_semaphore(SharedSemaphore::new(permits));
        self
    }
}

impl Layer for ConcurrentLimitLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(ConcurrentLimitService {
            limited: self.inner.apply_service(inner.clone()),
            inner,
            semaphore: self.semaphore.clone(),
        })
    }

    fn apply_context(&self, srv: Servicer, inner: OperationContext) -> OperationContext {
        self.inner.apply_context(srv, inner)
    }
}

#[derive(Debug)]
struct ConcurrentLimitService {
    inner: Servicer,
    limited: Servicer,
    semaphore: SharedSemaphore,
}

impl Service for ConcurrentLimitService {
    type Copier = oio::Copier;
    type Deleter = oio::Deleter;
    type Lister = ConcurrentLimitLister;
    type Reader = oio::Reader;
    type Writer = oio::Writer;

    fn info(&self) -> ServiceInfo {
        self.limited.info()
    }

    fn capability(&self) -> Capability {
        self.limited.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.limited.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.limited.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.limited.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.limited.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.limited.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        // Bypass only the upstream list wrapper, which holds a permit until drop.
        // The replacement uses the very same semaphore as all other operations.
        Ok(ConcurrentLimitLister {
            inner: self.inner.list(ctx, path, args)?,
            semaphore: self.semaphore.clone(),
        })
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        self.limited.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.limited.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.limited.presign(ctx, path, args).await
    }
}

struct ConcurrentLimitLister {
    inner: oio::Lister,
    semaphore: SharedSemaphore,
}

impl oio::List for ConcurrentLimitLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _permit = self.semaphore.acquire().await;
        self.inner.next().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{StreamExt, poll};
    use opendal::Operator;
    use opendal::raw::oio::List;
    use opendal::services::Memory;

    use super::*;

    #[tokio::test]
    async fn test_list_shares_operation_limit() {
        let layer = ConcurrentLimitLayer::new(1);
        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(layer.clone());
        op.write("file", "data").await.unwrap();
        let mut objects = op.lister_with("").recursive(true).await.unwrap();
        let permit = layer.semaphore.acquire().await;
        let mut next = Box::pin(objects.next());
        let mut stat = Box::pin(op.stat("file"));
        assert!(poll!(next.as_mut()).is_pending());
        assert!(poll!(stat.as_mut()).is_pending());
        drop(permit);
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(next.await.unwrap().unwrap().path(), "file");
            stat.await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(layer.semaphore.0.available_permits(), 1);
    }

    struct PendingLister;

    impl oio::List for PendingLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            std::future::pending().await
        }
    }

    struct FailingLister;

    impl oio::List for FailingLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "listing failed",
            ))
        }
    }

    #[tokio::test]
    async fn test_list_releases_permit_on_cancellation_and_error() {
        let layer = ConcurrentLimitLayer::new(1);
        let op = Operator::new(Memory::default())
            .unwrap()
            .layer(layer.clone());
        op.write("file", "data").await.unwrap();
        let mut objects = ConcurrentLimitLister {
            inner: Box::new(PendingLister),
            semaphore: layer.semaphore.clone(),
        };
        let mut next = Box::pin(objects.next());
        assert!(poll!(next.as_mut()).is_pending());
        assert_eq!(layer.semaphore.0.available_permits(), 0);
        let mut stat = Box::pin(op.stat("file"));
        assert!(poll!(stat.as_mut()).is_pending());
        drop(next);
        tokio::time::timeout(Duration::from_secs(5), stat)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(layer.semaphore.0.available_permits(), 1);

        let mut objects = ConcurrentLimitLister {
            inner: Box::new(FailingLister),
            semaphore: layer.semaphore.clone(),
        };
        assert!(objects.next().await.is_err());
        assert_eq!(layer.semaphore.0.available_permits(), 1);
    }
}
