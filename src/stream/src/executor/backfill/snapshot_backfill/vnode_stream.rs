// Copyright 2024 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::mem::{replace, take};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::stream::Peekable;
use futures::{Stream, StreamExt, TryStreamExt};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

use crate::executor::StreamExecutorResult;

pub(super) type BackfillRowItem = ((Op, OwnedRow), Option<(Op, OwnedRow)>);
pub(super) trait BackfillRowStream =
    Stream<Item = StreamExecutorResult<BackfillRowItem>> + Sized + 'static;

pub(super) struct VnodeStream<St: BackfillRowStream> {
    streams: HashMap<VirtualNode, Pin<Box<Peekable<St>>>>,
    finished_vnode: HashSet<VirtualNode>,
    data_chunk_builder: DataChunkBuilder,
    ops: Vec<Op>,
}

impl<St: BackfillRowStream> VnodeStream<St> {
    pub(super) fn new(
        vnode_streams: impl IntoIterator<Item = (VirtualNode, St)>,
        data_chunk_builder: DataChunkBuilder,
    ) -> Self {
        assert!(data_chunk_builder.is_empty());
        assert!(data_chunk_builder.batch_size() >= 2);
        let mut streams = HashMap::new();
        for (vnode, stream) in vnode_streams {
            let stream = Box::pin(stream.peekable());
            assert!(streams.insert(vnode, stream).is_none());
        }
        let ops = Vec::with_capacity(data_chunk_builder.batch_size());
        Self {
            streams,
            finished_vnode: HashSet::new(),
            data_chunk_builder,
            ops,
        }
    }
}

impl<St: BackfillRowStream> VnodeStream<St> {
    fn poll_next_row(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<StreamExecutorResult<Option<BackfillRowItem>>> {
        'outer: loop {
            if self.streams.is_empty() {
                break Poll::Ready(Ok(None));
            }
            for (vnode, stream) in &mut self.streams {
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Err(e))) => {
                        break 'outer Poll::Ready(Err(e));
                    }
                    Poll::Ready(None) => {
                        let vnode = *vnode;
                        let _stream = self.streams.remove(&vnode).expect("should exist");
                        self.finished_vnode.insert(vnode);
                        continue 'outer;
                    }
                    Poll::Ready(Some(Ok(row))) => {
                        break 'outer Poll::Ready(Ok(Some(row)));
                    }
                    Poll::Pending => {
                        continue;
                    }
                }
            }
            break Poll::Pending;
        }
    }

    pub(super) fn consume_builder(&mut self) -> Option<StreamChunk> {
        self.data_chunk_builder.consume_all().map(|chunk| {
            let ops = replace(
                &mut self.ops,
                Vec::with_capacity(self.data_chunk_builder.batch_size()),
            );
            StreamChunk::from_parts(ops, chunk)
        })
    }

    pub(super) async fn for_vnode_pk_progress(
        &mut self,
        pk_indices: &[usize],
        mut on_vnode_progress: impl FnMut(VirtualNode, Option<OwnedRow>),
    ) -> StreamExecutorResult<()> {
        assert!(self.data_chunk_builder.is_empty());
        for vnode in &self.finished_vnode {
            on_vnode_progress(*vnode, None);
        }
        for (vnode, stream) in &mut self.streams {
            match stream.as_mut().peek().await {
                Some(Ok(((_, row), extra))) => {
                    let pk = row.project(pk_indices).to_owned_row();
                    if cfg!(debug_assertions)
                        && let Some((_, extra_row)) = extra
                    {
                        assert_eq!(pk, extra_row.project(pk_indices).to_owned_row());
                    }
                    on_vnode_progress(*vnode, Some(pk));
                }
                Some(Err(_)) => {
                    return Err(stream.try_next().await.expect_err("checked Err"));
                }
                None => {
                    on_vnode_progress(*vnode, None);
                }
            }
        }
        Ok(())
    }
}

impl<St: BackfillRowStream> Stream for VnodeStream<St> {
    type Item = StreamExecutorResult<StreamChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let capacity = this.data_chunk_builder.batch_size();
        loop {
            match ready!(this.poll_next_row(cx)) {
                Ok(Some(((op, row), extra))) => {
                    let may_chunk = if let Some((extra_op, extra_row)) = extra {
                        if this.data_chunk_builder.can_append(2) {
                            this.ops.extend([op, extra_op]);
                            assert!(this.data_chunk_builder.append_one_row(row).is_none());
                            this.data_chunk_builder.append_one_row(extra_row)
                        } else {
                            let chunk = this
                                .data_chunk_builder
                                .consume_all()
                                .expect("should be Some when not can_append");
                            let ops = replace(&mut this.ops, Vec::with_capacity(capacity));
                            this.ops.extend([op, extra_op]);
                            assert!(this.data_chunk_builder.append_one_row(row).is_none());
                            assert!(this.data_chunk_builder.append_one_row(extra_row).is_none());
                            break Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))));
                        }
                    } else {
                        this.ops.push(op);
                        this.data_chunk_builder.append_one_row(row)
                    };
                    if let Some(chunk) = may_chunk {
                        let ops = replace(&mut this.ops, Vec::with_capacity(capacity));
                        break Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))));
                    }
                }
                Ok(None) => {
                    break if let Some(chunk) = this.data_chunk_builder.consume_all() {
                        let ops = take(&mut this.ops);
                        Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))))
                    } else {
                        Poll::Ready(None)
                    };
                }
                Err(e) => {
                    break Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}
