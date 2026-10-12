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

use risingwave_common::bail;
use risingwave_pb::plan_common::PbSourceRefreshMode;
use risingwave_pb::plan_common::source_refresh_mode::source_refresh_mode_bounded::Input as PbBoundedInput;
use risingwave_pb::plan_common::source_refresh_mode::{
    PbRefreshMode, SourceRefreshModeBounded as PbBounded,
    SourceRefreshModeFullReload as PbFullReload, SourceRefreshModeStreaming as PbStreaming,
};

use crate::error::ConnectorResult;
use crate::source::iceberg::IcebergBoundedScan;

/// How a source table ingests data. Decoded from [`PbSourceRefreshMode`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RefreshMode {
    /// Ingests continuously.
    #[default]
    Streaming,
    /// Reloads the whole source on `REFRESH TABLE`, or every `interval_sec` if set.
    FullReload { interval_sec: Option<i64> },
    /// Loads a pinned input once.
    Bounded(BoundedInput),
}

/// The input a bounded load reads, pinned when the table is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoundedInput {
    Iceberg(IcebergBoundedScan),
}

impl RefreshMode {
    pub fn from_proto(mode: Option<&PbSourceRefreshMode>) -> ConnectorResult<Self> {
        let Some(mode) = mode else {
            return Ok(Self::Streaming);
        };

        Ok(match &mode.refresh_mode {
            None | Some(PbRefreshMode::Streaming(_)) => Self::Streaming,
            Some(PbRefreshMode::FullReload(full_reload)) => Self::FullReload {
                interval_sec: full_reload.refresh_interval_sec,
            },
            Some(PbRefreshMode::Bounded(bounded)) => match bounded.input {
                Some(PbBoundedInput::Iceberg(scan)) => {
                    Self::Bounded(BoundedInput::Iceberg(IcebergBoundedScan::from_proto(scan)?))
                }
                None => bail!("bounded refresh mode requires a pinned input"),
            },
        })
    }

    pub fn to_proto(self) -> PbSourceRefreshMode {
        let refresh_mode = match self {
            Self::Streaming => PbRefreshMode::Streaming(PbStreaming {}),
            Self::FullReload { interval_sec } => PbRefreshMode::FullReload(PbFullReload {
                refresh_interval_sec: interval_sec,
            }),
            Self::Bounded(BoundedInput::Iceberg(scan)) => PbRefreshMode::Bounded(PbBounded {
                input: Some(PbBoundedInput::Iceberg(scan.to_proto())),
            }),
        };

        PbSourceRefreshMode {
            refresh_mode: Some(refresh_mode),
        }
    }

    pub fn is_full_reload(&self) -> bool {
        matches!(self, Self::FullReload { .. })
    }

    pub fn is_bounded(&self) -> bool {
        matches!(self, Self::Bounded(_))
    }

    /// The interval of periodic full reloads, if any.
    pub fn refresh_interval_sec(&self) -> Option<i64> {
        match self {
            Self::FullReload { interval_sec } => *interval_sec,
            Self::Streaming | Self::Bounded(_) => None,
        }
    }

    pub fn bounded_input(&self) -> Option<BoundedInput> {
        match self {
            Self::Bounded(input) => Some(*input),
            _ => None,
        }
    }

    pub fn iceberg_bounded_scan(&self) -> Option<IcebergBoundedScan> {
        self.bounded_input().map(|BoundedInput::Iceberg(scan)| scan)
    }
}

impl BoundedInput {
    pub fn iceberg_snapshot_id(&self) -> i64 {
        match self {
            Self::Iceberg(IcebergBoundedScan::Snapshot { snapshot_id }) => *snapshot_id,
        }
    }
}
