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

use clap::Parser;
use risingwave_ctl::CliOpts;
use risingwave_ctl::cmd_impl::log::LogWorkerSelector;
use risingwave_ctl::common::WorkerTypeArg;
use risingwave_pb::common::{WorkerNode, WorkerType};

#[test]
fn log_commands_require_explicit_unambiguous_targets() {
    for args in [
        vec![
            "log",
            "set",
            "info,risingwave_storage=debug",
            "--worker-id",
            "1,2",
        ],
        vec!["log", "set", "debug", "--node-id", "2"],
        vec!["log", "get", "--worker-type", "compute-node,frontend"],
        vec![
            "log",
            "get",
            "--worker-type",
            "compute",
            "--worker-type",
            "meta",
        ],
        vec!["log", "reset", "--worker-type", "compactor"],
        vec!["log", "reset", "--all"],
    ] {
        assert!(
            CliOpts::try_parse_from(std::iter::once("risectl").chain(args.clone())).is_ok(),
            "{args:?}"
        );
    }
    for args in [
        vec!["log", "set", "debug"],
        vec!["log", "reset"],
        vec!["log", "get", "--worker-type", "unknown"],
        vec![
            "log",
            "set",
            "debug",
            "--worker-id",
            "1",
            "--worker-type",
            "meta",
        ],
        vec!["log", "reset", "--all", "--worker-id", "1"],
    ] {
        assert!(
            CliOpts::try_parse_from(std::iter::once("risectl").chain(args.clone())).is_err(),
            "{args:?}"
        );
    }
}

fn workers() -> Vec<WorkerNode> {
    [
        (0, WorkerType::Meta),
        (1, WorkerType::Frontend),
        (2, WorkerType::ComputeNode),
        (3, WorkerType::ComputeNode),
        (4, WorkerType::Compactor),
    ]
    .into_iter()
    .map(|(id, kind)| WorkerNode {
        id: id.into(),
        r#type: kind.into(),
        ..Default::default()
    })
    .collect()
}

fn selected_ids(selector: LogWorkerSelector) -> Vec<u32> {
    selector
        .select(workers())
        .unwrap()
        .into_iter()
        .map(|worker| worker.id.as_raw_id())
        .collect()
}

#[test]
fn selects_ids_and_all_nodes_of_selected_types() {
    assert_eq!(
        selected_ids(LogWorkerSelector {
            worker_ids: vec![3, 0, 3],
            worker_types: vec![],
            all: false,
        }),
        vec![0, 3]
    );
    assert_eq!(
        selected_ids(LogWorkerSelector {
            worker_ids: vec![],
            worker_types: vec![
                WorkerTypeArg::ComputeNode,
                WorkerTypeArg::Frontend,
                WorkerTypeArg::ComputeNode
            ],
            all: false,
        }),
        vec![1, 2, 3]
    );
    assert_eq!(
        selected_ids(LogWorkerSelector {
            worker_ids: vec![],
            worker_types: vec![],
            all: true,
        }),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn rejects_missing_ids_and_empty_selection_before_updates() {
    let selector = LogWorkerSelector {
        worker_ids: vec![1, 100],
        worker_types: vec![],
        all: false,
    };
    assert!(
        selector
            .select(workers())
            .unwrap_err()
            .to_string()
            .contains("100")
    );
    let selector = LogWorkerSelector {
        worker_ids: vec![],
        worker_types: vec![WorkerTypeArg::Compactor],
        all: false,
    };
    assert!(selector.select(vec![]).is_err());
}
