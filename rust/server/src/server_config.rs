use std::fs;
use std::path::PathBuf;

use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::{IoError, UnknownError};
use risingwave_common::error::Result;
use serde::{Deserialize, Serialize};
use toml::Value;

const DEFAULT_HEARTBEAT_INTERVAL: u32 = 100;
const DEFAULT_CHUNK_SIZE: u32 = 1024;
const DEFAULT_SST_SIZE: u32 = 1024;
const DEFAULT_BLOCK_SIZE: u32 = 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    // For connection
    heartbeat_interval: u32,

    // Below for OLAP.
    // TODO: add more.

    // Below for streaming.
    // TODO: add more.
    chunk_size: u32,

    // Below for Hummock.
    // TODO: add more.
    sst_size: u32,
    block_size: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            chunk_size: DEFAULT_CHUNK_SIZE,
            sst_size: DEFAULT_SST_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl ServerConfig {
    pub fn heartbeat_interval(&self) -> u32 {
        self.heartbeat_interval
    }

    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    pub fn sst_size(&self) -> u32 {
        self.sst_size
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn init(path: PathBuf) -> Result<ServerConfig> {
        let config_str = fs::read_to_string(path).map_err(|e| RwError::from(IoError(e)))?;
        let cfg_value: Value = toml::from_str(config_str.as_str())
            .map_err(|_e| RwError::from(UnknownError("parse error".to_string())))?;
        let config = ServerConfig {
            heartbeat_interval: cfg_value["server"]["heartbeat_interval"]
                .as_integer()
                .unwrap() as u32,
            chunk_size: cfg_value["streaming"]["chunk_size"].as_integer().unwrap() as u32,
            sst_size: cfg_value["hummock"]["sst_size"].as_integer().unwrap() as u32,
            block_size: cfg_value["hummock"]["block_size"].as_integer().unwrap() as u32,
        };

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
        let cfg_value: Value = toml::from_str(config_str.as_str())
            .map_err(|_e| RwError::from(UnknownError("parse error".to_string())))?;
        assert_eq!(
            cfg_value["streaming"]["chunk_size"].as_integer().unwrap(),
            1024
        );
        Ok(())
    }
}
