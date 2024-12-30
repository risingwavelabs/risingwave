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

use std::collections::HashSet;
use std::mem::{replace, take};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::stream::{FuturesUnordered, Peekable, StreamFuture};
use futures::{Stream, StreamExt, TryStreamExt};
use pin_project::pin_project;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_storage::table::ChangeLogRow;

use crate::executor::StreamExecutorResult;

pub(super) trait ChangeLogRowStream =
    Stream<Item = StreamExecutorResult<ChangeLogRow>> + Sized + 'static;

#[pin_project]
struct StreamWithVnode<St> {
    #[pin]
    stream: St,
    vnode: VirtualNode,
}

impl<St: ChangeLogRowStream> Stream for StreamWithVnode<St> {
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

type ChangeLogRowVnodeStream<St> = Pin<Box<Peekable<StreamWithVnode<St>>>>;

pub(super) struct VnodeStream<St: ChangeLogRowStream> {
    streams: FuturesUnordered<StreamFuture<ChangeLogRowVnodeStream<St>>>,
    finished_vnode: HashSet<VirtualNode>,
    data_chunk_builder: DataChunkBuilder,
    ops: Vec<Op>,
}

impl<St: ChangeLogRowStream> VnodeStream<St> {
    pub(super) fn new(
        vnode_streams: impl IntoIterator<Item = (VirtualNode, St)>,
        data_chunk_builder: DataChunkBuilder,
    ) -> Self {
        assert!(data_chunk_builder.is_empty());
        assert!(data_chunk_builder.batch_size() >= 2);
        let streams =
            FuturesUnordered::from_iter(vnode_streams.into_iter().map(|(vnode, stream)| {
                Box::pin(StreamWithVnode { stream, vnode }.peekable()).into_future()
            }));
        let ops = Vec::with_capacity(data_chunk_builder.batch_size());
        Self {
            streams,
            finished_vnode: HashSet::new(),
            data_chunk_builder,
            ops,
        }
    }
}

impl<St: ChangeLogRowStream> VnodeStream<St> {
    fn poll_next_row(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<StreamExecutorResult<Option<ChangeLogRow>>> {
        loop {
            let ready_item = match ready!(self.streams.poll_next_unpin(cx)) {
                None => Ok(None),
                Some((None, stream)) => {
                    let stream = stream.get_ref();
                    assert!(self.finished_vnode.insert(stream.vnode));
                    continue;
                }
                Some((Some(Ok(item)), stream)) => {
                    // TODO: may avoid generating a `StreamFuture` for each row, because
                    // `FuturesUnordered::push` involve memory allocation of `Arc`, and may
                    // incur some unnecessary costs.
                    self.streams.push(stream.into_future());
                    Ok(Some(item))
                }
                Some((Some(Err(e)), _stream)) => Err(e),
            };
            break Poll::Ready(ready_item);
        }
    }

    #[expect(dead_code)]
    pub(super) fn consume_builder(&mut self) -> Option<StreamChunk> {
        self.data_chunk_builder.consume_all().map(|chunk| {
            let ops = replace(
                &mut self.ops,
                Vec::with_capacity(self.data_chunk_builder.batch_size()),
            );
            StreamChunk::from_parts(ops, chunk)
        })
    }

    #[expect(dead_code)]
    pub(super) async fn for_vnode_pk_progress(
        &mut self,
        pk_indices: &[usize],
        mut on_vnode_progress: impl FnMut(VirtualNode, Option<OwnedRow>),
    ) -> StreamExecutorResult<()> {
        assert!(self.data_chunk_builder.is_empty());
        for vnode in &self.finished_vnode {
            on_vnode_progress(*vnode, None);
        }
        for vnode_stream in &mut self.streams {
            let vnode_stream = vnode_stream.get_mut().expect("should exist");
            match vnode_stream.as_mut().peek().await {
                Some(Ok(change_log_row)) => {
                    let row = match change_log_row {
                        ChangeLogRow::Insert(row) | ChangeLogRow::Delete(row) => row,
                        ChangeLogRow::Update {
                            new_value,
                            old_value,
                        } => {
                            if cfg!(debug_assertions) {
                                assert_eq!(
                                    old_value.project(pk_indices),
                                    new_value.project(pk_indices)
                                );
                            }
                            new_value
                        }
                    };
                    let pk = row.project(pk_indices).to_owned_row();
                    let vnode_stream = vnode_stream.get_ref();
                    on_vnode_progress(vnode_stream.vnode, Some(pk));
                }
                Some(Err(_)) => {
                    return Err(vnode_stream.try_next().await.expect_err("checked Err"));
                }
                None => {
                    let vnode_stream = vnode_stream.get_ref();
                    on_vnode_progress(vnode_stream.vnode, None);
                }
            }
        }
        Ok(())
    }
}

impl<St: ChangeLogRowStream> Stream for VnodeStream<St> {
    type Item = StreamExecutorResult<StreamChunk>;

    // Here we implement the stream on our own instead of generating the stream with
    // `try_stream` macro, because we want to access the state of the streams on the flight.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let capacity = this.data_chunk_builder.batch_size();
        loop {
            match ready!(this.poll_next_row(cx)) {
                Ok(Some(change_log_row)) => {
                    let may_chunk = match change_log_row {
                        ChangeLogRow::Insert(row) => {
                            this.ops.push(Op::Insert);
                            this.data_chunk_builder.append_one_row(row)
                        }
                        ChangeLogRow::Delete(row) => {
                            this.ops.push(Op::Delete);
                            this.data_chunk_builder.append_one_row(row)
                        }
                        ChangeLogRow::Update {
                            new_value,
                            old_value,
                        } => {
                            if this.data_chunk_builder.can_append_update() {
                                this.ops.extend([Op::UpdateDelete, Op::UpdateInsert]);
                                assert!(this
                                    .data_chunk_builder
                                    .append_one_row(old_value)
                                    .is_none());
                                this.data_chunk_builder.append_one_row(new_value)
                            } else {
                                let chunk = this
                                    .data_chunk_builder
                                    .consume_all()
                                    .expect("should be Some when not can_append");
                                let ops = replace(&mut this.ops, Vec::with_capacity(capacity));
                                this.ops.extend([Op::UpdateDelete, Op::UpdateInsert]);
                                assert!(this
                                    .data_chunk_builder
                                    .append_one_row(old_value)
                                    .is_none());
                                assert!(this
                                    .data_chunk_builder
                                    .append_one_row(new_value)
                                    .is_none());
                                break Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))));
                            }
                        }
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
