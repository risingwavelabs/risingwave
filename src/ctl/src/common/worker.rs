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

use clap::ValueEnum;
use risingwave_pb::common::WorkerType;

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum WorkerTypeArg {
    Frontend,
    #[value(alias = "compute")]
    ComputeNode,
    Compactor,
    Meta,
}

impl From<WorkerTypeArg> for WorkerType {
    fn from(value: WorkerTypeArg) -> Self {
        match value {
            WorkerTypeArg::Frontend => Self::Frontend,
            WorkerTypeArg::ComputeNode => Self::ComputeNode,
            WorkerTypeArg::Compactor => Self::Compactor,
            WorkerTypeArg::Meta => Self::Meta,
        }
    }
}
