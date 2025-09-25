use std::mem::transmute;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use clap::Parser;
use foyer::{CacheBuilder, HybridCacheBuilder};
use risingwave_common::array::{Finite32, VectorRef};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::dispatch_distance_measurement;
use risingwave_common::types::F32;
use risingwave_common::vector::distance::DistanceMeasurement;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::vector_index::{HnswFlatIndex, HnswFlatIndexAdd};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_pb::common::PbDistanceType;
use risingwave_pb::hummock::query_vectors_response::NearestNeighbors;
use risingwave_pb::hummock::vector_index_service_server::{
    VectorIndexService, VectorIndexServiceServer,
};
use risingwave_pb::hummock::{
    InitIndexRequest, InitIndexResponse, InsertVectorsRequest, InsertVectorsResponse,
    QueryVectorsRequest, QueryVectorsResponse,
};
use risingwave_storage::hummock::none::NoneRecentFilter;
use risingwave_storage::hummock::vector::file::FileVectorStore;
use risingwave_storage::hummock::vector::writer::{HnswFlatIndexWriter, VectorObjectIdManager};
use risingwave_storage::hummock::{
    HummockResult, SstableStore, SstableStoreConfig, SstableStoreRef,
};
use risingwave_storage::monitor::HummockStateStoreMetrics;
use risingwave_storage::opts::StorageOpts;
use risingwave_storage::vector::Vector;
use risingwave_storage::vector::hnsw::nearest;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, async_trait};

#[derive(clap::Parser, Clone, Debug)]
pub struct Opts {
    #[clap(long)]
    pub listen_addr: String,

    #[clap(long)]
    pub parallelism: usize,

    #[clap(long)]
    pub block_size_kb: usize,

    #[clap(long)]
    pub block_cache_capacity_mb: usize,

    #[clap(long)]
    pub meta_cache_capacity_mb: usize,
}

enum VectorRequest {
    Init(InitIndexRequest, oneshot::Sender<()>),
    Insert(InsertVectorsRequest, oneshot::Sender<()>),
    Query(QueryVectorsRequest, oneshot::Sender<QueryVectorsResponse>),
}

struct VectorIndexServiceImpl {
    tx: tokio::sync::mpsc::UnboundedSender<VectorRequest>,
}

struct ObjectIdManager {
    next_object_id: AtomicU64,
}

#[async_trait]
impl VectorObjectIdManager for ObjectIdManager {
    async fn get_new_vector_object_id(&self) -> HummockResult<HummockRawObjectId> {
        let id = self
            .next_object_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(HummockRawObjectId::new(id))
    }
}

async fn sstable_store(opts: &Opts) -> SstableStoreRef {
    let object_store = build_remote_object_store(
        "memory",
        Arc::new(ObjectStoreMetrics::unused()),
        "Hummock",
        Arc::new(ObjectStoreConfig::default()),
    )
    .await;
    Arc::new(SstableStore::new(SstableStoreConfig {
        store: Arc::new(object_store),
        path: "hummock".to_owned(),
        prefetch_buffer_capacity: 0,
        max_prefetch_block_number: 0,
        recent_filter: Arc::new(NoneRecentFilter::default().into()),
        state_store_metrics: Arc::new(HummockStateStoreMetrics::unused()),
        use_new_object_prefix_strategy: false,
        meta_cache: HybridCacheBuilder::new()
            .memory(1 << 20)
            .with_shards(16)
            .storage()
            .build()
            .await
            .unwrap(),
        block_cache: HybridCacheBuilder::new()
            .memory(1 << 20)
            .with_shards(16)
            .storage()
            .build()
            .await
            .unwrap(),
        vector_meta_cache: CacheBuilder::new(opts.meta_cache_capacity_mb * (1 << 20)).build(),
        vector_block_cache: CacheBuilder::new(opts.block_cache_capacity_mb * (1 << 20)).build(),
    }))
}

async fn init(
    rx: &mut UnboundedReceiver<VectorRequest>,
    sstable_store: SstableStoreRef,
    opts: &Opts,
) -> (HnswFlatIndexWriter, HnswFlatIndex, DistanceMeasurement) {
    let VectorRequest::Init(req, tx) = rx.recv().await.unwrap() else {
        unreachable!()
    };
    let index = HnswFlatIndex::new(req.config.as_ref().unwrap());
    let object_id_manager = Arc::new(ObjectIdManager {
        next_object_id: AtomicU64::new(0),
    }) as Arc<dyn VectorObjectIdManager>;
    println!("Initializing index with request: {:?}", req);
    tx.send(()).unwrap();

    let storage_opts = StorageOpts {
        vector_file_block_size_kb: opts.block_size_kb,
        ..Default::default()
    };
    let measurement =
        DistanceMeasurement::from(PbDistanceType::try_from(req.distance_type).unwrap());
    let writer = HnswFlatIndexWriter::new(
        &index,
        req.dimension as usize,
        measurement,
        sstable_store,
        object_id_manager,
        &storage_opts,
    )
    .await
    .unwrap();
    (writer, index, measurement)
}

async fn train(
    rx: &mut UnboundedReceiver<VectorRequest>,
    mut writer: HnswFlatIndexWriter,
) -> HnswFlatIndexAdd {
    let mut next_vector_id: u64 = 0;
    loop {
        let VectorRequest::Insert(req, tx) = rx.recv().await.unwrap() else {
            unreachable!()
        };
        for vec in req.insert_vectors {
            let vector_id = next_vector_id;
            next_vector_id += 1;
            let info = Bytes::copy_from_slice(&vector_id.to_le_bytes());
            // #safety: Finish32 is transparent to f32
            let values: Vec<Finite32> = unsafe { transmute(vec.values) };
            let vec = Vector::from(values);
            writer.insert(vec, info).unwrap();
        }
        println!("Inserted {next_vector_id} vectors");
        writer.try_flush().await.unwrap();
        tx.send(()).unwrap();
        if req.is_finished {
            break;
        }
    }
    writer.flush().await.unwrap();
    writer.seal_current_epoch().expect("non-empty")
}

async fn query(
    rx: &mut UnboundedReceiver<VectorRequest>,
    sstable_store: SstableStoreRef,
    hnsw_flat: HnswFlatIndex,
    measurement: DistanceMeasurement,
) {
    let graph_file = hnsw_flat.graph_file.as_ref().unwrap();
    let graph = sstable_store.get_hnsw_graph(graph_file).await.unwrap();
    let vector_store = FileVectorStore::new_for_reader(&hnsw_flat, sstable_store.clone());
    while let Some(req) = rx.recv().await {
        let VectorRequest::Query(req, tx) = req else {
            unreachable!()
        };
        let mut results = Vec::with_capacity(req.query_vectors.len());
        for query_vec in req.query_vectors {
            let (vector_ids, _stats) = dispatch_distance_measurement!(measurement, M, {
                nearest::<_, M>(
                    &vector_store,
                    &*graph,
                    VectorRef::from_slice_unchecked(
                        // #safety: F32 is transparent to f32
                        unsafe { transmute::<&[f32], &[F32]>(query_vec.values.as_slice()) },
                    ),
                    |_, _, info| u64::from_le_bytes(info.try_into().unwrap()),
                    req.ef_search as _,
                    req.top_n as _,
                )
                .await
                .unwrap()
            });
            results.push(NearestNeighbors { vector_ids });
        }
        tx.send(QueryVectorsResponse { results }).unwrap();
    }
}

async fn worker(mut rx: UnboundedReceiver<VectorRequest>, opts: Opts) {
    let sstable_store = sstable_store(&opts).await;
    let (writer, mut index, measurement) = init(&mut rx, sstable_store.clone(), &opts).await;
    let add = train(&mut rx, writer).await;
    index.apply_hnsw_flat_index_add(&add);
    query(&mut rx, sstable_store, index, measurement).await;
}

#[async_trait]
impl VectorIndexService for VectorIndexServiceImpl {
    async fn init_index(
        &self,
        request: Request<InitIndexRequest>,
    ) -> Result<Response<InitIndexResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(VectorRequest::Init(request.into_inner(), tx))
            .map_err(|_| Status::internal("Failed to send init request"))?;

        rx.await
            .map_err(|_| Status::internal("Failed to receive init response"))?;
        Ok(Response::new(InitIndexResponse {}))
    }

    async fn insert_vectors(
        &self,
        request: Request<InsertVectorsRequest>,
    ) -> Result<Response<InsertVectorsResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(VectorRequest::Insert(request.into_inner(), tx))
            .map_err(|_| Status::internal("Failed to send insert request"))?;

        rx.await
            .map_err(|_| Status::internal("Failed to receive insert response"))?;
        Ok(Response::new(InsertVectorsResponse {}))
    }

    async fn query_vectors(
        &self,
        request: Request<QueryVectorsRequest>,
    ) -> Result<Response<QueryVectorsResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(VectorRequest::Query(request.into_inner(), tx))
            .map_err(|_| Status::internal("Failed to send query request"))?;

        let response = rx
            .await
            .map_err(|_| Status::internal("Failed to receive query response"))?;
        Ok(Response::new(response))
    }
}

fn main() {
    let opts = Opts::parse();
    println!("opts: {:?}", opts);
    let (tx, rx) = unbounded_channel();
    let service = VectorIndexServiceServer::new(VectorIndexServiceImpl { tx });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(opts.parallelism)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    let join1 = runtime.spawn(worker(rx, opts.clone()));
    let join2 = runtime.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve(opts.listen_addr.parse().unwrap())
            .await
            .unwrap()
    });

    runtime.block_on(async move {
        tokio::try_join!(join1, join2).unwrap();
    });
}
