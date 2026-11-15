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

use risingwave_connector::source::ingest_mode::IngestMode;
use risingwave_pb::catalog::source::Kind as PbSourceKind;
use risingwave_pb::catalog::{PbHybridSource, PbHybridStage};

use crate::catalog::SourceId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceKind {
    Connector(IngestMode),
    Hybrid(Vec<SourceId>),
}

impl SourceKind {
    pub fn from_proto(kind: Option<&PbSourceKind>) -> Self {
        match kind {
            Some(PbSourceKind::Hybrid(hybrid)) => Self::Hybrid(hybrid.stage_source_ids()),
            Some(PbSourceKind::IngestMode(mode)) => {
                let mode =
                    IngestMode::from_proto(Some(mode)).expect("stored ingest mode should be valid");

                Self::Connector(mode)
            }
            None => Self::Connector(IngestMode::Streaming),
        }
    }

    pub fn to_proto(&self) -> PbSourceKind {
        match self {
            Self::Connector(mode) => PbSourceKind::IngestMode(mode.to_proto()),
            Self::Hybrid(stages) => {
                let stages = stages
                    .iter()
                    .map(|&source_id| PbHybridStage { source_id })
                    .collect();

                PbSourceKind::Hybrid(PbHybridSource { stages })
            }
        }
    }

    pub fn ingest_mode(&self) -> Option<IngestMode> {
        match self {
            Self::Connector(mode) => Some(*mode),
            Self::Hybrid(_) => None,
        }
    }

    pub fn is_streaming(&self) -> bool {
        self.ingest_mode().is_some_and(|mode| mode.is_streaming())
    }

    pub fn is_full_reload(&self) -> bool {
        self.ingest_mode().is_some_and(|mode| mode.is_full_reload())
    }

    pub fn hybrid_stages(&self) -> Option<&[SourceId]> {
        match self {
            Self::Connector(_) => None,
            Self::Hybrid(stages) => Some(stages),
        }
    }

    pub fn is_hybrid(&self) -> bool {
        matches!(self, Self::Hybrid(_))
    }
}
