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

use risingwave_pb::stream_plan::{PbThrottle, PbThrottleKind};

/// Throttle policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Throttle {
    /// Throttle disabled.
    Disabled,
    /// Throttle with fixed rate.
    Fixed(u32),
}

impl Throttle {
    /// Convert from protobuf.
    pub fn from_protobuf(pb: PbThrottle) -> Self {
        match pb.kind() {
            PbThrottleKind::Disabled => Self::Disabled,
            PbThrottleKind::Fixed => Self::Fixed(pb.rate),
        }
    }

    /// Convert into protobuf.
    pub fn to_protobuf(self) -> PbThrottle {
        match self {
            Throttle::Disabled => PbThrottle {
                kind: PbThrottleKind::Disabled.into(),
                ..Default::default()
            },
            Throttle::Fixed(rate) => PbThrottle {
                kind: PbThrottleKind::Fixed.into(),
                rate,
            },
        }
    }

    /// Return if the throttle is set to rate 0.
    pub fn is_zero(&self) -> bool {
        match self {
            Throttle::Fixed(0) => true,
            _ => false,
        }
    }

    // /// Get the throttled max chunk size.
    // pub fn max_chunk_size(&self) -> usize {
    //     let configured = crate::config::chunk_size();
    //     match self {
    //         Throttle::Disabled => configured,
    //         Throttle::Fixed(rate) => configured.min(*rate as usize),
    //     }
    // }
}

impl From<PbThrottle> for Throttle {
    fn from(pb: PbThrottle) -> Self {
        Self::from_protobuf(pb)
    }
}

/// Convert from the old repersentation.
impl From<Option<u32>> for Throttle {
    fn from(value: Option<u32>) -> Self {
        match value {
            Some(rate) => Self::Fixed(rate),
            None => Self::Disabled,
        }
    }
}
