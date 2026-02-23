# AGENTS.md - Docker Configurations

## 1. Scope

Policies for the docker directory, covering Docker images, compose files, and container orchestration for RisingWave deployment.

## 2. Purpose

The docker module provides production-ready Docker images and compose configurations for deploying RisingWave in containerized environments. It supports various storage backends including MinIO, S3, GCS, Azure Blob, and HDFS.

## 3. Structure

```
docker/
├── Dockerfile                    # Main RisingWave image definition
├── Dockerfile.hdfs              # HDFS-enabled image variant
├── docker-compose.yml           # Standalone MinIO backend
├── docker-compose-with-s3.yml   # S3 backend configuration
├── docker-compose-with-gcs.yml  # Google Cloud Storage backend
├── docker-compose-with-azblob.yml  # Azure Blob Storage backend
├── docker-compose-with-oss.yml  # Alibaba OSS backend
├── docker-compose-with-obs.yml  # Huawei OBS backend
├── docker-compose-with-hdfs.yml # HDFS backend
├── docker-compose-with-local-fs.yml  # Local filesystem backend
├── docker-compose-with-sqlite.yml    # SQLite backend
├── docker-compose-with-lakekeeper.yml # Lakekeeper integration
├── docker-compose-distributed.yml    # Distributed cluster setup
├── aws/                         # AWS-specific configurations
├── aws.env                      # AWS credentials template
├── multiple_object_storage.env  # Multi-cloud storage config
├── dashboards/                  # Grafana dashboard configurations
├── grafana.ini                  # Grafana configuration
├── grafana-risedev-dashboard.yml    # Dashboard provisioning
├── grafana-risedev-datasource.yml   # Datasource provisioning
├── prometheus.yaml              # Prometheus configuration
├── risingwave.toml              # RisingWave server config
├── hdfs_env.sh                  # HDFS environment setup
├── crypto.sql                   # Cryptographic functions
└── README.md                    # Usage documentation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Main RisingWave container image |
| `docker-compose.yml` | Quick start with MinIO |
| `docker-compose-with-*.yml` | Backend-specific deployments |
| `aws.env` | AWS credentials configuration |
| `multiple_object_storage.env` | Multi-cloud storage settings |
| `grafana.ini` | Monitoring dashboard config |
| `prometheus.yaml` | Metrics collection config |

## 5. Edit Rules (Must)

- Update README.md when adding new compose files
- Test all compose files before committing changes
- Document memory requirements for each configuration
- Include health checks in all service definitions
- Use specific image tags instead of `latest` in production configs
- Document required environment variables
- Follow Docker best practices (layer caching, minimal images)
- Test compose files with `docker-compose config` validation

## 6. Forbidden Changes (Must Not)

- Remove backward compatibility for existing compose files
- Hardcode credentials in Dockerfiles or compose files
- Use privileged containers without justification
- Expose unnecessary ports
- Modify published image tags retroactively
- Remove supported storage backend configurations
- Break multi-arch image support (x86_64, aarch64)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build image | `docker build -f docker/Dockerfile .` |
| Validate compose | `docker-compose -f <file> config` |
| Integration test | `docker-compose -f <file> up` |
| Multi-arch build | `docker buildx build --platform linux/amd64,linux/arm64` |

## 8. Dependencies & Contracts

- Docker Engine 20.10+
- Docker Compose 2.0+
- Container registries (GHCR, Docker Hub)
- Base images (Rust builder, Debian/Ubuntu runtime)
- SIMD support (AVX2 for x86_64, NEON for aarch64)

## 9. Overrides

Inherits from `/home/k11/risingwave/AGENTS.md`:
- Override: Test Entry - Docker-specific validation required
- Override: Dependencies - Container runtime dependencies

## 10. Update Triggers

Regenerate this file when:
- New storage backends are supported
- Docker image structure changes
- Compose file organization changes
- New deployment patterns are introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md
