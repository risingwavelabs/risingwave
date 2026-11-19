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

use std::mem::take;
use std::num::NonZeroU64;

use anyhow::Context;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_connector::sink::decouple_checkpoint_log_sink::{
    COMMIT_CHECKPOINT_INTERVAL, ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL,
};
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::MetaResult;
use crate::barrier::BarrierKind;
use crate::barrier::info::BarrierInfo;

/// Combines upstream checkpoints before writers flush and the PK index is committed.
/// Recovery starts a fresh interval, replaying everything after the last committed checkpoint.
pub(super) struct CommitCheckpointInterval {
    interval: NonZeroU64,
    checkpoint_count: u64,
    pending_epochs: Vec<u64>,
}

impl CommitCheckpointInterval {
    pub(super) fn from_fragment_nodes<'a>(
        nodes: impl IntoIterator<Item = &'a PbStreamNode>,
    ) -> MetaResult<Self> {
        for root in nodes {
            let mut sink_desc = None;
            visit_stream_node_cont(root, |node| {
                if let Some(NodeBody::IcebergWithPkIndexWriter(writer)) = &node.node_body {
                    sink_desc = writer.sink_desc.as_ref();
                }
                sink_desc.is_none()
            });
            if let Some(sink_desc) = sink_desc {
                let mut interval = Self {
                    interval: NonZeroU64::new(ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL).unwrap(),
                    checkpoint_count: 0,
                    pending_epochs: vec![],
                };
                interval.update(
                    sink_desc
                        .properties
                        .get(COMMIT_CHECKPOINT_INTERVAL)
                        .map(String::as_str),
                )?;
                return Ok(interval);
            }
        }
        Err(anyhow::anyhow!("Iceberg V3 job has no writer sink description").into())
    }

    pub(super) fn update(&mut self, value: Option<&str>) -> MetaResult<()> {
        if let Some(value) = value {
            // Preserve skipped epochs and the elapsed interval across ALTER.
            self.interval = value
                .parse()
                .context("invalid `commit_checkpoint_interval`")?;
        }
        Ok(())
    }

    pub(super) fn transform(&mut self, mut barrier: BarrierInfo, force: bool) -> BarrierInfo {
        if let BarrierKind::Checkpoint(epochs) = &mut barrier.kind {
            self.pending_epochs.append(epochs);
            self.checkpoint_count += 1;
            barrier.kind = if force || self.checkpoint_count >= self.interval.get() {
                self.checkpoint_count = 0;
                BarrierKind::Checkpoint(take(&mut self.pending_epochs))
            } else {
                BarrierKind::Barrier
            };
        }
        barrier
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::Epoch;
    use risingwave_pb::stream_plan::{IcebergWithPkIndexWriterNode, SinkDesc};

    use super::*;
    use crate::barrier::TracedEpoch;

    fn barrier_info(prev: u64, curr: u64, kind: BarrierKind) -> BarrierInfo {
        BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch::from_physical_time(prev)),
            curr_epoch: TracedEpoch::new(Epoch::from_physical_time(curr)),
            kind,
        }
    }

    fn checkpoint_interval(interval: Option<&str>) -> MetaResult<CommitCheckpointInterval> {
        let writer = PbStreamNode {
            node_body: Some(NodeBody::IcebergWithPkIndexWriter(Box::new(
                IcebergWithPkIndexWriterNode {
                    sink_desc: Some(SinkDesc {
                        properties: interval
                            .map(|value| (COMMIT_CHECKPOINT_INTERVAL.to_owned(), value.to_owned()))
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        };
        // The writer need not be the fragment root.
        let root = PbStreamNode {
            input: vec![writer],
            ..Default::default()
        };
        CommitCheckpointInterval::from_fragment_nodes([&root])
    }

    #[test]
    fn test_commit_checkpoint_interval_config() {
        assert_eq!(checkpoint_interval(Some("3")).unwrap().interval.get(), 3);
        assert_eq!(
            checkpoint_interval(None).unwrap().interval.get(),
            ICEBERG_DEFAULT_COMMIT_CHECKPOINT_INTERVAL
        );
        assert!(checkpoint_interval(Some("0")).is_err());
    }

    #[test]
    fn test_commit_every_n_checkpoints() {
        let mut checkpoint_interval = checkpoint_interval(Some("3")).unwrap();
        for (prev, kind) in [
            (10, BarrierKind::Barrier),
            (12, BarrierKind::Checkpoint(vec![10, 12])),
            (14, BarrierKind::Checkpoint(vec![14])),
            (16, BarrierKind::Barrier),
        ] {
            let barrier = checkpoint_interval.transform(barrier_info(prev, prev + 2, kind), false);
            assert_eq!(barrier.kind, BarrierKind::Barrier);
        }
        let barrier = checkpoint_interval.transform(
            barrier_info(18, 20, BarrierKind::Checkpoint(vec![16, 18])),
            false,
        );
        assert_eq!(
            barrier.kind,
            BarrierKind::Checkpoint(vec![10, 12, 14, 16, 18])
        );
    }

    #[test]
    fn test_decrease_interval_preserves_pending_epochs() {
        let mut interval = checkpoint_interval(Some("5")).unwrap();
        for prev in [10, 12] {
            let barrier = interval.transform(
                barrier_info(prev, prev + 2, BarrierKind::Checkpoint(vec![prev])),
                false,
            );
            assert_eq!(barrier.kind, BarrierKind::Barrier);
        }
        interval.update(Some("1")).unwrap();
        let barrier = interval.transform(
            barrier_info(14, 16, BarrierKind::Checkpoint(vec![14])),
            false,
        );
        assert_eq!(barrier.kind, BarrierKind::Checkpoint(vec![10, 12, 14]));
        let barrier = interval.transform(
            barrier_info(16, 18, BarrierKind::Checkpoint(vec![16])),
            false,
        );
        assert_eq!(barrier.kind, BarrierKind::Checkpoint(vec![16]));
    }

    #[test]
    fn test_increase_interval_preserves_checkpoint_count() {
        let mut interval = checkpoint_interval(Some("2")).unwrap();
        interval.transform(
            barrier_info(10, 12, BarrierKind::Checkpoint(vec![10])),
            false,
        );
        interval.update(Some("3")).unwrap();
        let barrier = interval.transform(
            barrier_info(12, 14, BarrierKind::Checkpoint(vec![12])),
            false,
        );
        assert_eq!(barrier.kind, BarrierKind::Barrier);
        let barrier = interval.transform(
            barrier_info(14, 16, BarrierKind::Checkpoint(vec![14])),
            false,
        );
        assert_eq!(barrier.kind, BarrierKind::Checkpoint(vec![10, 12, 14]));
    }

    #[test]
    fn test_unrelated_properties_preserve_interval_progress() {
        let mut interval = checkpoint_interval(Some("2")).unwrap();
        interval.transform(
            barrier_info(10, 12, BarrierKind::Checkpoint(vec![10])),
            false,
        );
        interval.update(None).unwrap();
        assert_eq!(interval.interval.get(), 2);
        let barrier = interval.transform(
            barrier_info(12, 14, BarrierKind::Checkpoint(vec![12])),
            false,
        );
        assert_eq!(barrier.kind, BarrierKind::Checkpoint(vec![10, 12]));
    }

    #[test]
    fn test_invalid_interval_update_preserves_state() {
        let mut interval = checkpoint_interval(Some("2")).unwrap();
        interval.transform(
            barrier_info(10, 12, BarrierKind::Checkpoint(vec![10])),
            false,
        );
        for value in ["0", "-1", "invalid"] {
            assert!(interval.update(Some(value)).is_err());
            assert_eq!(interval.interval.get(), 2);
            assert_eq!(interval.checkpoint_count, 1);
            assert_eq!(interval.pending_epochs, vec![10]);
        }
    }

    #[test]
    fn test_forced_checkpoint_drains_and_resets_interval() {
        let mut checkpoint_interval = checkpoint_interval(Some("2")).unwrap();
        checkpoint_interval.transform(
            barrier_info(10, 12, BarrierKind::Checkpoint(vec![10])),
            false,
        );
        // Snapshot completion and compaction Begin must include skipped checkpoints.
        let forced = checkpoint_interval.transform(
            barrier_info(12, 14, BarrierKind::Checkpoint(vec![12, 14])),
            true,
        );
        assert_eq!(forced.kind, BarrierKind::Checkpoint(vec![10, 12, 14]));
        // Compaction End remains a checkpoint even immediately after Begin.
        let end = checkpoint_interval.transform(
            barrier_info(14, 16, BarrierKind::Checkpoint(vec![15])),
            true,
        );
        assert_eq!(end.kind, BarrierKind::Checkpoint(vec![15]));
        assert_eq!(
            checkpoint_interval
                .transform(
                    barrier_info(16, 18, BarrierKind::Checkpoint(vec![16])),
                    false,
                )
                .kind,
            BarrierKind::Barrier
        );
        assert_eq!(
            checkpoint_interval
                .transform(
                    barrier_info(18, 20, BarrierKind::Checkpoint(vec![18])),
                    false,
                )
                .kind,
            BarrierKind::Checkpoint(vec![16, 18])
        );
    }
}
