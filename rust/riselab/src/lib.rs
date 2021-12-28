#![feature(exit_status_error)]

mod compute_node_service;
mod frontend_service;
mod meta_node_service;
mod minio_service;
mod task_configure_grpc_node;
mod task_configure_minio;
pub use compute_node_service::*;
pub use frontend_service::*;
pub use meta_node_service::*;
pub use minio_service::*;
pub use task_configure_grpc_node::*;
pub use task_configure_minio::*;
mod configure_tmux_service;
pub mod util;
mod wait_tcp;
pub use configure_tmux_service::*;

pub trait Task: 'static + Send {
    fn execute(&mut self) -> anyhow::Result<u32>;
    fn id(&self) -> String;
}
