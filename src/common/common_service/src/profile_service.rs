// Copyright 2025 RisingWave Labs
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

use std::ffi::CString;
use std::fs;
use std::path::Path;

use itertools::Itertools;
use risingwave_common::config::ServerConfig;
use risingwave_common_heap_profiling::{AUTO_DUMP_SUFFIX, COLLAPSED_SUFFIX, MANUALLY_DUMP_SUFFIX};
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, HeapProfilingRequest, HeapProfilingResponse,
    ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest, ProfilingResponse,
};
use risingwave_rpc_client::error::ToTonicStatus as _;
use thiserror_ext::AsReport;
use tokio::time::Duration;
use tonic::{Code, Request, Response, Status};

/// Implementation of the profiling related services in `MonitorService`.
/// Can be reused to implement the same services in different types of worker nodes.
#[derive(Clone)]
pub struct ProfileServiceImpl {
    server_config: ServerConfig,
}

#[allow(clippy::unused_async)]
impl ProfileServiceImpl {
    pub fn new(server_config: ServerConfig) -> Self {
        Self { server_config }
    }

    pub async fn profiling(
        &self,
        request: Request<ProfilingRequest>,
    ) -> Result<Response<ProfilingResponse>, Status> {
        if std::env::var("RW_PROFILE_PATH").is_ok() {
            return Err(Status::internal(
                "Profiling is already running by setting RW_PROFILE_PATH",
            ));
        }
        let time = request.into_inner().get_sleep_s();
        let guard = pprof::ProfilerGuardBuilder::default()
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();
        tokio::time::sleep(Duration::from_secs(time)).await;
        let mut buf = vec![];
        match guard.report().build() {
            Ok(report) => {
                report.flamegraph(&mut buf).unwrap();
                tracing::info!("succeed to generate flamegraph");
                Ok(Response::new(ProfilingResponse { result: buf }))
            }
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to generate flamegraph");
                Err(err.to_status(Code::Internal, "monitor"))
            }
        }
    }

    pub async fn heap_profiling(
        &self,
        request: Request<HeapProfilingRequest>,
    ) -> Result<Response<HeapProfilingResponse>, Status> {
        use std::fs::create_dir_all;
        use std::path::PathBuf;

        use tikv_jemalloc_ctl;

        if !cfg!(target_os = "linux") {
            return Err(Status::unimplemented(
                "heap profiling is only implemented on Linux",
            ));
        }

        if !tikv_jemalloc_ctl::opt::prof::read().unwrap() {
            return Err(Status::failed_precondition(
                "Jemalloc profiling is not enabled on the node. Try start the node with `MALLOC_CONF=prof:true`",
            ));
        }

        let time_prefix = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S");
        let file_name = format!("{}.{}", time_prefix, MANUALLY_DUMP_SUFFIX);
        let arg_dir = request.into_inner().dir;
        let dir = PathBuf::from(if arg_dir.is_empty() {
            &self.server_config.heap_profiling.dir
        } else {
            &arg_dir
        });
        create_dir_all(&dir)?;

        let file_path_buf = dir.join(file_name);
        let file_path = file_path_buf
            .to_str()
            .ok_or_else(|| Status::internal("The file dir is not a UTF-8 String"))?;
        let file_path_c =
            CString::new(file_path).map_err(|_| Status::internal("0 byte in file path"))?;

        // FIXME(yuhao): `unsafe` here because `jemalloc_dump_mib.write` requires static lifetime
        if let Err(e) =
            tikv_jemalloc_ctl::prof::dump::write(unsafe { &*(file_path_c.as_c_str() as *const _) })
        {
            tracing::warn!("Manually Jemalloc dump heap file failed! {:?}", e);
            Err(Status::internal(e.to_string()))
        } else {
            tracing::info!("Manually Jemalloc dump heap file created: {}", file_path);
            Ok(Response::new(HeapProfilingResponse {}))
        }
    }

    pub async fn list_heap_profiling(
        &self,
        _request: Request<ListHeapProfilingRequest>,
    ) -> Result<Response<ListHeapProfilingResponse>, Status> {
        let dump_dir = self.server_config.heap_profiling.dir.clone();
        let auto_dump_files_name: Vec<_> = fs::read_dir(dump_dir.clone())?
            .map(|entry| {
                let entry = entry?;
                Ok::<_, Status>(entry.file_name().to_string_lossy().to_string())
            })
            .filter(|name| {
                if let Ok(name) = name {
                    name.contains(AUTO_DUMP_SUFFIX) && !name.ends_with(COLLAPSED_SUFFIX)
                } else {
                    true
                }
            })
            .try_collect()?;
        let manually_dump_files_name: Vec<_> = fs::read_dir(dump_dir.clone())?
            .map(|entry| {
                let entry = entry?;
                Ok::<_, Status>(entry.file_name().to_string_lossy().to_string())
            })
            .filter(|name| {
                if let Ok(name) = name {
                    name.contains(MANUALLY_DUMP_SUFFIX) && !name.ends_with(COLLAPSED_SUFFIX)
                } else {
                    true
                }
            })
            .try_collect()?;

        Ok(Response::new(ListHeapProfilingResponse {
            dir: dump_dir,
            name_auto: auto_dump_files_name,
            name_manually: manually_dump_files_name,
        }))
    }

    pub async fn analyze_heap(
        &self,
        request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        let dumped_path_str = request.into_inner().get_path().clone();
        let collapsed_path_str = format!("{}.{}", dumped_path_str, COLLAPSED_SUFFIX);
        let collapsed_path = Path::new(&collapsed_path_str);

        // run jeprof if the target was not analyzed before
        if !collapsed_path.exists() {
            risingwave_common_heap_profiling::jeprof::run(
                dumped_path_str,
                collapsed_path_str.clone(),
            )
            .await
            .map_err(|e| e.to_status(Code::Internal, "monitor"))?;
        }

        let file = fs::read(Path::new(&collapsed_path_str))?;
        Ok(Response::new(AnalyzeHeapResponse { result: file }))
    }
}
