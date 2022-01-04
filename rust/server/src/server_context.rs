use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use risingwave_common::error::ErrorCode::MetaError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::addr::get_host_port;
use risingwave_meta::rpc::meta_client::MetaClient;

use crate::server_config::ServerConfig;

const DEFAULT_WORKER_ID: u32 = 1;
const DEFAULT_LOG4RS_CONFIG: &str = "config/log4rs.yaml";
const DEFAULT_HOST: &str = "127.0.0.1:5688";
const DEFAULT_STATE_STORE: &str = "in-memory";
const DEFAULT_PROMETHEUS_LISTENER_ADDRESS: &str = "0.0.0.0:1222";
const DEFAULT_META_ADDRESS: &str = "http://127.0.0.1:5690";

pub struct ServerContextCore {
    worker_id: u32,
    log4rs_config: String,
    host: String,
    state_store: String,
    prometheus_listener_address: String,
    meta_address: String,
}

impl Default for ServerContextCore {
    fn default() -> Self {
        ServerContextCore {
            worker_id: DEFAULT_WORKER_ID,
            log4rs_config: DEFAULT_LOG4RS_CONFIG.to_owned(),
            host: DEFAULT_HOST.to_owned(),
            state_store: DEFAULT_STATE_STORE.to_owned(),
            prometheus_listener_address: DEFAULT_PROMETHEUS_LISTENER_ADDRESS.to_owned(),
            meta_address: DEFAULT_META_ADDRESS.to_owned(),
        }
    }
}

impl ServerContextCore {
    pub fn set_worker_id(&mut self, id: u32) {
        self.worker_id = id;
    }

    pub fn get_worker_id(&self) -> u32 {
        self.worker_id
    }

    pub fn set_log4rs_config(&mut self, log4rs_config: &str) {
        self.log4rs_config = log4rs_config.to_owned();
    }

    pub fn get_log4rs_config(&self) -> &str {
        self.log4rs_config.as_str()
    }

    pub fn set_host(&mut self, host: &str) {
        self.host = host.to_owned();
    }

    pub fn get_host(&self) -> &str {
        self.host.as_str()
    }

    pub fn set_state_store(&mut self, state_store: &str) {
        self.state_store = state_store.to_owned();
    }

    pub fn get_state_store(&self) -> &str {
        self.state_store.as_str()
    }

    pub fn set_prometheus_listener_address(&mut self, prometheus_listener_address: &str) {
        self.prometheus_listener_address = prometheus_listener_address.to_owned();
    }

    pub fn get_prometheus_listener_address(&self) -> &str {
        self.prometheus_listener_address.as_str()
    }

    pub fn set_meta_address(&mut self, meta_address: &str) {
        self.meta_address = meta_address.to_owned();
    }

    pub fn get_meta_address(&self) -> &str {
        self.meta_address.as_str()
    }
}

pub struct ServerContext {
    // Immutable part.
    meta_client: MetaClient,
    config: ServerConfig,
    // Mutable part.
    core: Mutex<ServerContextCore>,
}

impl ServerContext {
    pub async fn new(meta_address: &str, config_path: &str) -> Self {
        let res = MetaClient::new(meta_address).await;
        match res {
            Ok(meta_client) => {
                let config_path = PathBuf::from(config_path.to_owned());
                debug!("config path, {:?}", fs::canonicalize(&config_path));
                let config = ServerConfig::init(config_path).unwrap();
                let context = ServerContext {
                    meta_client,
                    config,
                    core: Mutex::new(ServerContextCore::default()),
                };
                // Initialize some parameters.
                context.set_meta_address(meta_address);
                context
            }
            Err(e) => {
                panic!("meta connection error: {}", e)
            }
        }
    }

    pub async fn register_to_meta(&self) -> Result<()> {
        let addr = get_host_port(self.get_host().as_str()).unwrap();
        let res = self.meta_client.register(addr).await;
        match res {
            Ok(id) => {
                debug!("register as worker:{:?}", id);
                self.set_worker_id(id);
                Ok(())
            }
            _ => Err(RwError::from(MetaError(
                "Error while registering to meta.".to_string(),
            ))),
        }
    }

    pub fn get_config(&self) -> ServerConfig {
        self.config.clone()
    }

    pub fn set_worker_id(&self, id: u32) {
        let mut core = self.core.lock().unwrap();
        core.set_worker_id(id);
    }

    pub fn get_worker_id(&self) -> u32 {
        self.core.lock().unwrap().get_worker_id()
    }

    pub fn set_log4rs_config(&self, log4rs_config: &str) {
        let mut core = self.core.lock().unwrap();
        core.set_log4rs_config(log4rs_config);
    }

    pub fn get_log4rs_config(&self) -> String {
        self.core.lock().unwrap().get_log4rs_config().to_string()
    }

    pub fn set_host(&self, host: &str) {
        let mut core = self.core.lock().unwrap();
        core.set_host(host);
    }

    pub fn get_host(&self) -> String {
        self.core.lock().unwrap().get_host().to_string()
    }

    pub fn set_state_store(&self, state_store: &str) {
        let mut core = self.core.lock().unwrap();
        core.set_state_store(state_store);
    }

    pub fn get_state_store(&self) -> String {
        self.core.lock().unwrap().get_state_store().to_string()
    }

    pub fn set_prometheus_listener_address(&self, prometheus_listener_address: &str) {
        let mut core = self.core.lock().unwrap();
        core.set_prometheus_listener_address(prometheus_listener_address);
    }

    pub fn get_prometheus_listener_address(&self) -> String {
        self.core
            .lock()
            .unwrap()
            .get_prometheus_listener_address()
            .to_string()
    }

    pub fn set_meta_address(&self, meta_address: &str) {
        let mut core = self.core.lock().unwrap();
        core.set_meta_address(meta_address);
    }

    pub fn get_meta_address(&self) -> String {
        self.core.lock().unwrap().get_meta_address().to_string()
    }
}
