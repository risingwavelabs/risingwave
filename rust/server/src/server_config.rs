use std::fs;
use std::path::PathBuf;

use risingwave_common::array::{InternalError, RwError};
use risingwave_common::error::ErrorCode::IoError;
use risingwave_common::error::Result;
use serde::{Deserialize, Serialize};

const DEFAULT_HEARTBEAT_INTERVAL: u32 = 100;
const DEFAULT_CHUNK_SIZE: u32 = 1024;
const DEFAULT_SST_SIZE: u32 = 1024;
const DEFAULT_BLOCK_SIZE: u32 = 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComputeNodeConfig {
    // For connection
    server: ServerConfig,

    // Below for OLAP.
    olap: OlapConfig,

    // Below for streaming.
    streaming: StreamingConfig,

    // Below for Hummock.
    hummock: HummockConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServerConfig {
    #[serde(default = "default_heartbeat_interval")]
    heartbeat_interval: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OlapConfig {
    #[serde(default = "default_chunk_size")]
    chunk_size: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamingConfig {
    #[serde(default = "default_chunk_size")]
    chunk_size: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HummockConfig {
    #[serde(default = "default_sst_size")]
    sst_size: u32,

    #[serde(default = "default_block_size")]
    block_size: u32,
}

fn default_heartbeat_interval() -> u32 {
    DEFAULT_HEARTBEAT_INTERVAL
}

fn default_chunk_size() -> u32 {
    DEFAULT_CHUNK_SIZE
}

fn default_sst_size() -> u32 {
    DEFAULT_SST_SIZE
}

fn default_block_size() -> u32 {
    DEFAULT_BLOCK_SIZE
}

impl ComputeNodeConfig {
    pub fn heartbeat_interval(&self) -> u32 {
        self.server.heartbeat_interval
    }

    pub fn olap_chunk_size(&self) -> u32 {
        self.olap.chunk_size
    }

    pub fn streaming_chunk_size(&self) -> u32 {
        self.streaming.chunk_size
    }

    pub fn sst_size(&self) -> u32 {
        self.hummock.sst_size
    }

    pub fn hummock_block_size(&self) -> u32 {
        self.hummock.block_size
    }

    pub fn init(path: PathBuf) -> Result<ComputeNodeConfig> {
        let config_str = fs::read_to_string(path).map_err(|e| RwError::from(IoError(e)))?;
        let config: ComputeNodeConfig = toml::from_str(config_str.as_str())
            .map_err(|e| RwError::from(InternalError(format!("parse error {}", e))))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_from_file() -> Result<()> {
        let path = PathBuf::from("./../config/risingwave.toml");
        println!("first, {:?}", fs::canonicalize(&path));
        let config_str = std::fs::read_to_string(path).map_err(|e| RwError::from(IoError(e)))?;
        let config: ComputeNodeConfig = toml::from_str(config_str.as_str())
            .map_err(|e| RwError::from(InternalError(format!("parse error {}", e))))?;
        assert_eq!(config.hummock_block_size(), 1024);
        Ok(())
    }
}
