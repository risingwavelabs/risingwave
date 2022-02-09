use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

const DEFAULT_HEARTBEAT_INTERVAL: u32 = 100;
const DEFAULT_CHUNK_SIZE: u32 = 1024;
const DEFAULT_SST_SIZE: u32 = 1024;
const DEFAULT_BLOCK_SIZE: u32 = 1024;

/// TODO(TaoWu): The configs here may be preferable to be managed under corresponding module
/// separately.

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ComputeNodeConfig {
    // For connection
    #[serde(default)]
    server: ServerConfig,

    // Below for batch query.
    #[serde(default)]
    batch: BatchConfig,

    // Below for streaming.
    #[serde(default)]
    streaming: StreamingConfig,

    // Below for Hummock.
    #[serde(default)]
    hummock: HummockConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServerConfig {
    heartbeat_interval: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}

impl ServerConfig {
    pub fn heartbeat_interval(&self) -> u32 {
        self.heartbeat_interval
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    chunk_size: u32,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl BatchConfig {
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamingConfig {
    chunk_size: u32,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl StreamingConfig {
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HummockConfig {
    sst_size: u32,

    block_size: u32,
}

impl Default for HummockConfig {
    fn default() -> Self {
        Self {
            sst_size: DEFAULT_SST_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl HummockConfig {
    pub fn sst_size(&self) -> u32 {
        self.sst_size
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }
}

impl ComputeNodeConfig {
    pub fn init(path: PathBuf) -> Result<ComputeNodeConfig> {
        let config_str = fs::read_to_string(path.clone()).map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to open config file '{}': {}",
                path.to_string_lossy(),
                e
            )))
        })?;
        let config: ComputeNodeConfig = toml::from_str(config_str.as_str())
            .map_err(|e| RwError::from(InternalError(format!("parse error {}", e))))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_default() {
        use super::*;

        let cfg = ComputeNodeConfig::default();
        assert_eq!(cfg.heartbeat_interval(), DEFAULT_HEARTBEAT_INTERVAL);

        let cfg: ComputeNodeConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.hummock_block_size(), DEFAULT_BLOCK_SIZE);
    }
}
