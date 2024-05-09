// The templates used as steps in RiseDev profiles.

{
  minio: {
    // Id of this instance
    id: 'minio',
    // Advertise address of MinIO s3 endpoint
    address: '127.0.0.1',
    // Advertise port of MinIO s3 endpoint
    port: 9301,
    // Listen address of MinIO endpoint
    listenAddress: self.address,
    // Console address of MinIO s3 endpoint
    consoleAddress: '127.0.0.1',
    // Console port of MinIO s3 endpoint
    consolePort: 9400,
    // Root username (can be used to login to MinIO console)
    rootUser: 'hummockadmin',
    // Root password (can be used to login to MinIO console)
    rootPassword: 'hummockadmin',
    // Bucket name to store hummock information
    hummockBucket: 'hummock001',
    // Prometheus nodes used by this MinIO
    providePrometheus: 'prometheus*',
    // Max concurrent api requests.
    // see: https://github.com/minio/minio/blob/master/docs/throttle/README.md.
    // '0' means this env var will use the default of minio.
    apiRequestsMax: 0,
    // Deadline for api requests.
    // Empty string means this env var will use the default of minio.
    apiRequestsDeadline: '',
  },

  etcd: {
    // Id of this instance
    id: 'etcd-' + self.port,
    // Advertise address of the single-node etcd.
    address: '127.0.0.1',
    // Listen port of the single-node etcd.
    port: 2388,
    // Listen address
    listenAddress: self.address,
    // Peer listen port of the single-node etcd.
    peerPort: 2389,
    // Prometheus exporter listen port
    exporterPort: 2379,
    // Whether to enable fsync (NEVER SET TO TRUE IN PRODUCTION ENVIRONMENT!)
    unsafeNoFsync: false,
    // Other etcd nodes
    provideEtcd: 'etcd*',
  },

  sqlite: {
    // Id of this instance
    id: 'sqlite',
    // File name of the sqlite database
    file: 'metadata.db',
  },

  computeNode: {
    // Id of this instance
    id: 'compute-node-' + self.port,
    // Compute-node advertise address
    address: '127.0.0.1',
    // Listen address
    listenAddress: self.address,
    // Compute-node listen port
    port: 5688,
    // Prometheus exporter listen port
    exporterPort: 1222,
    // Whether to enable async stack trace for this compute node, `off`, `on`, or `verbose`.
    // Considering the performance, `verbose` mode only effect under `release` profile with `debug_assertions` off.
    asyncStackTrace: 'verbose',
    // If `enable-tiered-cache` is true, hummock will use data directory as file cache.
    enableTieredCache: false,
    // Minio instances used by this compute node
    provideMinio: 'minio*',
    // OpenDAL storage backend used by this compute node
    provideOpendal: 'opendal*',
    // AWS s3 bucket used by this compute node
    provideAwsS3: 'aws-s3*',
    // Meta-nodes used by this compute node
    provideMetaNode: 'meta-node*',
    // Tempo used by this compute node
    provideTempo: 'tempo*',
    // If `user-managed` is true, this service will be started by user with the above config
    userManaged: false,
    // Total available memory for the compute node in bytes
    totalMemoryBytes: 8589934592,
    // Parallelism of tasks per compute node
    parallelism: 4,
    role: 'both',
  },

  metaNode: {
    // Id of this instance
    id: 'meta-node-' + self.port,
    // Meta-node advertise address
    address: '127.0.0.1',
    // Meta-node listen port
    port: 5690,
    // Listen address
    listenAddress: self.address,
    // Dashboard listen port
    dashboardPort: 5691,
    // Prometheus exporter listen port
    exporterPort: 1250,
    // If `user-managed` is true, this service will be started by user with the above config
    userManaged: false,
    // Etcd backend config
    provideEtcdBackend: 'etcd*',
    // Sqlite backend config
    provideSqliteBackend: 'sqlite*',
    // Prometheus nodes used by dashboard service
    providePrometheus: 'prometheus*',
    // Sanity check: should use shared storage if there're multiple compute nodes
    provideComputeNode: 'compute-node*',
    // Sanity check: should start at lease one compactor if using shared object store
    provideCompactor: 'compactor*',
    // Minio instances used by the cluster
    provideMinio: 'minio*',
    // OpenDAL storage backend used by the cluster
    provideOpendal: 'opendal*',
    // AWS s3 bucket used by the cluster
    provideAwsS3: 'aws-s3*',
    // Tempo used by this meta node
    provideTempo: 'tempo*',
    // Whether to enable in-memory pure KV state backend
    enableInMemoryKvStateBackend: false,
  },

  prometheus: {
    // Id of this instance
    id: 'prometheus',
    // Advertise address of Prometheus
    address: '127.0.0.1',
    // Listen port of Prometheus
    port: 9500,
    // Listen address
    listenAddress: self.address,
    // If `remote_write` is true, this Prometheus instance will push metrics to remote instance
    remoteWrite: false,
    // AWS region of remote write
    remoteWriteRegion: '',
    // Remote write url of this instance
    remoteWriteUrl: '',
    // Compute-nodes used by this Prometheus instance
    provideComputeNode: 'compute-node*',
    // Meta-nodes used by this Prometheus instance
    provideMetaNode: 'meta-node*',
    // Minio instances used by this Prometheus instance
    provideMinio: 'minio*',
    // Compactors used by this Prometheus instance
    provideCompactor: 'compactor*',
    // Etcd used by this Prometheus instance
    provideEtcd: 'etcd*',
    // Redpanda used by this Prometheus instance
    provideRedpanda: 'redpanda*',
    // Frontend used by this Prometheus instance
    provideFrontend: 'frontend*',
    // How frequently Prometheus scrape targets (collect metrics)
    scrapeInterval: '15s',
  },

  frontend: {
    // Id of this instance
    id: 'frontend-' + self.port,
    // Advertise address of frontend
    address: '127.0.0.1',
    // Listen port of frontend
    port: 4566,
    // Listen address
    listenAddress: self.address,
    // Prometheus exporter listen port
    exporterPort: 2222,
    // Health check listen port
    healthCheckPort: 6786,
    // Meta-nodes used by this frontend instance
    provideMetaNode: 'meta-node*',
    // Tempo used by this frontend instance
    provideTempo: 'tempo*',
    // If `user-managed` is true, this service will be started by user with the above config
    userManaged: false,
  },

  compactor: {
    // Id of this instance
    id: 'compactor-' + self.port,
    // Compactor advertise address
    address: '127.0.0.1',
    // Compactor listen port
    port: 6660,
    // Listen address
    listenAddress: self.address,
    // Prometheus exporter listen port
    exporterPort: 1260,
    // Minio instances used by this compactor
    provideMinio: 'minio*',
    // Meta-nodes used by this compactor
    provideMetaNode: 'meta-node*',
    // Tempo used by this compator
    provideTempo: 'tempo*',
    // If `user-managed` is true, this service will be started by user with the above config
    userManaged: false,
  },

  grafana: {
    // Id of this instance
    id: 'grafana',
    // Listen address of Grafana
    listenAddress: self.address,
    // Advertise address of Grafana
    address: '127.0.0.1',
    // Listen port of Grafana
    port: 3001,
    // Prometheus used by this Grafana instance
    providePrometheus: 'prometheus*',
    // Tempo used by this Grafana instance
    provideTempo: 'tempo*',
  },

  tempo: {
    // Id of this instance
    id: 'tempo',
    // Listen address of HTTP server and OTLP gRPC collector
    listenAddress: '127.0.0.1',
    // Advertise address of Tempo
    address: '127.0.0.1',
    // HTTP server listen port
    port: 3200,
    // gRPC listen port of the OTLP collector
    otlpPort: 4317,
    maxBytesPerTrace: 5000000,
  },

  opendal: {
    id: 'opendal',
    engine: 'hdfs',
    namenode: '127.0.0.1:9000',
    bucket: 'risingwave-test',
  },

  // awsS3 is a placeholder service to provide configurations
  awsS3: {
    // Id to be picked-up by services
    id: 'aws-s3',
    // The bucket to be used for AWS S3
    bucket: 'test-bucket',
    // access key, secret key and region should be set in aws config (either by env var or .aws/config)
  },

  // Apache Kafka service
  kafka: {
    // Id to be picked-up by services
    id: 'kafka-' + self.port,
    // Advertise address of Kafka
    address: '127.0.0.1',
    // Listen port of Kafka
    port: 29092,
    // Listen address
    listenAddress: self.address,
    // ZooKeeper used by this Kafka instance
    provideZookeeper: 'zookeeper*',
    // If set to true, data will be persisted at data/{id}.
    persistData: true,
    // Kafka broker id. If there are multiple instances of Kafka, we will need to set.
    brokerId: 0,
    userManaged: false,
  },

  // Google pubsub emulator service
  pubsub: {
    id: 'pubsub-' + self.port,
    address: '127.0.0.1',
    port: 5980,
    persistData: true,
  },

  // Apache ZooKeeper service
  zookeeper: {
    // Id to be picked-up by services
    id: 'zookeeper-' + self.port,
    // Advertise address of ZooKeeper
    address: '127.0.0.1',
    // Listen address
    listenAddress: self.address,
    // Listen port of ZooKeeper
    port: 2181,
    // If set to true, data will be persisted at data/{id}.
    persistData: true,
  },

  // Only supported in RiseDev compose
  redpanda: {
    // Id to be picked-up by services
    id: 'redpanda',
    // Port used inside docker-compose cluster (e.g. create MV)
    internalPort: 29092,
    // Port used on host (e.g. import data, connecting using kafkacat)
    outsidePort: 9092,
    // Connect address
    address: self.id,
    // Number of CPUs to use
    cpus: 8,
    // Memory limit for Redpanda
    memory: '16G',
  },

  // redis service
  redis: {
    // Id to be picked-up by services
    id: 'redis',
    // listen port of redis
    port: 6379,
    // address of redis
    address: '127.0.0.1',
  },

  // MySQL service backed by docker.
  mysql: {
    // Id to be picked-up by services
    id: 'mysql-' + self.port,
    // address of mysql
    address: '127.0.0.1',
    // listen port of mysql
    port: 8306,
    // Note:
    // - This will be used to initialize the MySQL instance if it's fresh.
    // - In user-managed mode, these configs are not validated by risedev.
    //   They are passed as-is to risedev-env default user for MySQL operations.
    // - This is not used in RISEDEV_MYSQL_WITH_OPTIONS_COMMON.
    user: 'root',
    password: '',
    database: 'risedev',
    // The docker image. Can be overridden to use a different version.
    image: 'mysql:8',
    // If set to true, data will be persisted at data/{id}.
    persistData: true,
    // If `user-managed` is true, user is responsible for starting the service
    // to serve at the above address and port in any way they see fit.
    userManaged: false,
  },
}
