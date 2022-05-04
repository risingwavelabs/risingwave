// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generate docker compose yaml files for risedev components.

use std::collections::HashMap;
use std::process::Command;

use anyhow::Result;
use serde::Serialize;

use crate::{
    ComputeNodeConfig, ComputeNodeService, FrontendConfig, FrontendServiceV2, MetaNodeConfig,
    MetaNodeService,
};

#[derive(Debug, Clone, Serialize, Default)]
pub struct ComposeService {
    pub image: String,
    pub command: Vec<String>,
    pub expose: Vec<String>,
    pub ports: Vec<String>,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComposeFile {
    pub version: String,
    pub services: HashMap<String, ComposeService>,
}

/// Generate compose yaml files for a component.
pub trait Compose {
    fn compose(&self) -> Result<ComposeService>;
}

impl Compose for ComputeNodeConfig {
    fn compose(&self) -> Result<ComposeService> {
        let mut command = Command::new("/risingwave/bin/compute-node");
        ComputeNodeService::apply_command_args(&mut command, self)?;
        let command = command
            .get_args()
            .map(|x| x.to_owned().to_string_lossy().to_string())
            .collect();

        Ok(ComposeService {
            image: "ghcr.io/singularity-data/risingwave:latest".into(),
            command,
            expose: vec![self.port.to_string(), self.exporter_port.to_string()],
            depends_on: self
                .provide_meta_node
                .as_ref()
                .unwrap()
                .iter()
                .map(|x| x.id.clone())
                .collect(),
            ports: vec![],
            ..Default::default()
        })
    }
}

impl Compose for MetaNodeConfig {
    fn compose(&self) -> Result<ComposeService> {
        let mut command = Command::new("/risingwave/bin/meta-node");
        MetaNodeService::apply_command_args(&mut command, self)?;
        let command = command
            .get_args()
            .map(|x| x.to_owned().to_string_lossy().to_string())
            .collect();

        Ok(ComposeService {
            image: "ghcr.io/singularity-data/risingwave:latest".into(),
            command,
            expose: vec![
                self.port.to_string(),
                self.exporter_port.to_string(),
                self.dashboard_port.to_string(),
            ],
            ports: vec![],
            ..Default::default()
        })
    }
}

impl Compose for FrontendConfig {
    fn compose(&self) -> Result<ComposeService> {
        let mut command = Command::new("/risingwave/bin/frontend-v2");
        FrontendServiceV2::apply_command_args(&mut command, self)?;
        let command = command
            .get_args()
            .map(|x| x.to_owned().to_string_lossy().to_string())
            .collect();

        Ok(ComposeService {
            image: "ghcr.io/singularity-data/risingwave:latest".into(),
            command,
            ports: vec![format!("{}:{}", self.port, self.port)],
            expose: vec![self.port.to_string()],
            ..Default::default()
        })
    }
}
