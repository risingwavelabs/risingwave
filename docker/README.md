# RisingWave Docker Images

## Run RisingWave with Docker Compose

Docker Compose allows you to easily launch a RisingWave instance on a single node. If you are using more than one server, please refer to [Deploy RisingWave on Kubernetes](https://docs.risingwave.com/deploy/risingwave-kubernetes).


Ensure you are using the latest version of RisingWave image:

```
# Ensure risingwave image is of latest version
docker pull ghcr.io/risingwavelabs/risingwave:latest
```

### Playground

Playground mode does not persist any data. **Never** use it for production purpose.

To start a RisingWave playground, run

```
# Start playground
docker run -it --pull=always -p 4566:4566 -p 5691:5691 ghcr.io/risingwavelabs/risingwave:latest playground
```

### Standalone (MinIO backend)

To start a RisingWave standalone instance with MinIO backend, run

```
# Start all components
docker-compose up
```

**⚠️ Important Notes: Memory is crucial for RisingWave!** Inappropriate memory configuration may lead to OOM (out-of-memory) errors or poor performance. Before deploying Docker Compose, ensure that the correct memory settings are configured in the `docker-compose.yaml` file. Here are examples of some typical settings.

| Memory for RW container (`resource.limits.memory`) | 8 GiB | 14 GiB | 28 GiB | 58 GiB |
|----------------------------------------------------|---|---|---|---|
| `compute-opts.total-memory-bytes`                  | 6 GiB | 10 GiB | 20 GiB | 46 GiB |
| `frontend-opts.frontend-total-memory-bytes`        | 1 GiB | 2 GiB | 4 GiB | 6 GiB |
| `compactor-opts.compactor-total-memory-bytes`      | 1 GiB | 2 GiB | 4 GiB | 6 GiB |
| `compute-opts.memory-manager-target-bytes`         | 5.6 GiB | 9.8 GiB | 20.8 GiB | 44.8 GiB |


### Standalone (S3 backend)

To start a RisingWave cluster with s3 backend, configure the aws credit in [aws.env](https://github.com/risingwavelabs/risingwave/blob/main/docker/aws.env).
If you want to use some s3 compatible storage like Tencent Cloud COS, just configure one more [endpoint](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/aws.env#L7).
After configuring the environment and fill in your [bucket name](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-s3.yml#L196), run

```
# Start all components
docker-compose -f docker-compose-with-s3.yml up
```

It will run with s3 (compatible) object storage with a meta node, a compute node, a frontend, a compactor, a prometheus and a redpanda instance.

### Standalone (other backend)

To start a RisingWave cluster with other storage backend, like Google Cloud Storage, Alicloud OSS or Azure Blob Storage, configure the authentication information in [multiple_object_storage.env](https://github.com/risingwavelabs/risingwave/blob/main/docker/multiple_object_storage.env), fill in your [bucket name](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-gcs.yml#L196).
and run

```
# Start all components
docker-compose -f docker-compose-with-xxx.yml up
```

It will run RisingWave with corresponding (object) storage products.

> [!NOTE]
>
> For RisingWave kernel hackers, we always recommend using [risedev](../src/risedevtool/README.md) to start the full cluster, instead of using docker images.
> See [CONTRIBUTING](../CONTRIBUTING.md) for more information.


## Published images

- `nightly`, latest nightly build
- `latest`, latest stable version
- Other tags available on both GHCR and Docker Hub:
  - `nightly-yyyyMMdd`, e.g., `nightly-20230108`
  - `vX.Y.Z-alpha.yyyyMMdd`, e.g., `v2.3.0-alpha.20250301`
  - `vX.Y.Z`, e.g., `v0.1.15`

## Build the images

The default images use AVX2 on x86_64 and NEON on aarch64.

To build the images, simply run:

```
docker build . -f docker/Dockerfile --build-arg CARGO_PROFILE=production
```

from the project root.

### ARM CPU targets

Set `RW_TARGET_CPU` explicitly for the deployment hardware. The default `generic`
keeps the existing ARM build flags. Different instance sizes using the same CPU
can share an image.

| `RW_TARGET_CPU` | Deployment hardware | CI image tag suffix |
| --- | --- | --- |
| `generic` (default) | Existing supported ARM64 machines | `-aarch64` |
| `neoverse-n1` | Graviton2 and newer | `-aarch64-neoverse-n1` |
| `neoverse-v1` | Graviton3 | `-aarch64-neoverse-v1` |
| `neoverse-v2` | Graviton4 | `-aarch64-neoverse-v2` |

For example, build a Graviton3 image on an ARM64 builder:

```bash
docker build . -f docker/Dockerfile \
  --build-arg CARGO_PROFILE=production \
  --build-arg RW_TARGET_CPU=neoverse-v1 \
  -t risingwave:local-aarch64-neoverse-v1
```

The parameter appends a Rust CPU target to the repository's Cargo flags. It does
not set global C/C++ compiler flags or change Java and prebuilt libraries. The
image records the target in the `io.risingwave.target-cpu` label. Build scripts and
proc macros retain the host CPU target.

For the Docker Buildkite pipeline (`ci/workflows/docker.yml`), set the build
environment variable `RW_TARGET_CPU=neoverse-v1`. This selects the ARM image path;
x86 builds, binary releases and image scanning are skipped. CPU-specific staging
tags and registry build caches get the CPU suffix. Published aliases also get the
suffix, for example `git-<commit>-neoverse-v1` or `vX.Y.Z-neoverse-v1`, and contain
only the ARM image. The default workflow keeps its existing tags and architectures.
The nightly/tag-release workflow is unchanged; use the Docker pipeline for these
opt-in builds.

The builder used for the startup smoke test must support the chosen target. The
existing `linux-arm64` queue does not itself guarantee Graviton3/4 hardware; select
a compatible agent before running a CPU-specific build. A failed startup prevents
publication. Deployment must likewise select a matching image and constrain its
node pool; the standard multi-architecture manifest does not choose CPU models.
The CPU suffixes are reserved, and the publishing scripts reject retags that add,
remove or change a CPU requirement.

## Generate docker-compose.yml

```bash
./risedev compose
```

## Common Issues

Error message:
```
Error { code: "XMinioStorageFull", message: "Storage backend has reached its minimum free drive threshold. Please delete a few objects to proceed."
```

Solution:

This usually happens on MacOS with Docker Desktop. The Docker Deskup runs in the macOS Hypervisor. All the data, including logs, images, volumes, and so on, is stored in this hypervisor and the hypervisor has a default disk capacity limit. So when this message emerges, simply cleaning up the unused container or image can help mitigate. You can also increase capacity limit by clicking the Docker Desktop icon in the menu bar, then clicking Preferences > Resources > `Increase Disk image size`.
