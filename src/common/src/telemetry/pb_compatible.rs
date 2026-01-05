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

use risingwave_pb::telemetry::{
    ReportBase as PbTelemetryReportBase, SystemCpu as PbSystemCpu, SystemData as PbSystemData,
    SystemMemory as PbSystemMemory, SystemOs as PbSystemOs,
    TelemetryNodeType as PbTelemetryNodeType,
};

use crate::telemetry::{Cpu, Memory, Os, SystemData, TelemetryNodeType, TelemetryReportBase};

pub trait TelemetryToProtobuf {
    fn to_pb_bytes(self) -> Vec<u8>;
}

impl From<TelemetryReportBase> for PbTelemetryReportBase {
    fn from(val: TelemetryReportBase) -> Self {
        PbTelemetryReportBase {
            tracking_id: val.tracking_id,
            session_id: val.session_id,
            system_data: Some(val.system_data.into()),
            up_time: val.up_time,
            report_time: val.time_stamp,
            node_type: from_telemetry_node_type(val.node_type) as i32,
            is_test: val.is_test,
        }
    }
}

fn from_telemetry_node_type(t: TelemetryNodeType) -> PbTelemetryNodeType {
    match t {
        TelemetryNodeType::Meta => PbTelemetryNodeType::Meta,
        TelemetryNodeType::Compute => PbTelemetryNodeType::Compute,
        TelemetryNodeType::Frontend => PbTelemetryNodeType::Frontend,
        TelemetryNodeType::Compactor => PbTelemetryNodeType::Compactor,
    }
}

impl From<TelemetryNodeType> for PbTelemetryNodeType {
    fn from(val: TelemetryNodeType) -> Self {
        match val {
            TelemetryNodeType::Meta => PbTelemetryNodeType::Meta,
            TelemetryNodeType::Compute => PbTelemetryNodeType::Compute,
            TelemetryNodeType::Frontend => PbTelemetryNodeType::Frontend,
            TelemetryNodeType::Compactor => PbTelemetryNodeType::Compactor,
        }
    }
}

impl From<Cpu> for PbSystemCpu {
    fn from(val: Cpu) -> Self {
        PbSystemCpu {
            available: val.available,
        }
    }
}

impl From<Memory> for PbSystemMemory {
    fn from(val: Memory) -> Self {
        PbSystemMemory {
            used: val.used as u64,
            total: val.total as u64,
        }
    }
}

impl From<Os> for PbSystemOs {
    fn from(val: Os) -> Self {
        PbSystemOs {
            name: val.name,
            kernel_version: val.kernel_version,
            version: val.version,
        }
    }
}

impl From<SystemData> for PbSystemData {
    fn from(val: SystemData) -> Self {
        PbSystemData {
            memory: Some(val.memory.into()),
            os: Some(val.os.into()),
            cpu: Some(val.cpu.into()),
        }
    }
}
