# Docker Images

## Published images

- `latest` on GHCR (latest nightly build): `ghcr.io/risingwavelabs/risingwave:latest`
- `latest` on Docker Hub (latest release): `risingwavelabs/risingwave:latest`
- Other tags available on both GHCR and Docker Hub:
  - `nightly-yyyyMMdd`, e.g., `nightly-20230108`
  - `vX.Y.Z`, e.g., `v0.1.15`

## Build the images

The docker images for x86_64 are built with AVX2 SIMD extensions, while the images for aarch64 are built with NEON SIMD extensions. These must be available on your machine. If your machine does not support these extensions, you must build the docker image with the build-arg `simd_disabled=true`.

To build the images, simply run:

```
docker build . -f docker/Dockerfile
```

from the project root.

To build the images without SIMD vector extensions, run

```
docker build . -f docker/Dockerfile --build-arg simd_disabled=true
```

from the project root and run any subsequent docker commands on the resultant image.

## Use the images

To ensure you are using the latest version of RisingWave image,

```
# Ensure risingwave image is of latest version
docker pull ghcr.io/risingwavelabs/risingwave:latest
```

### playground
To start a RisingWave playground, run

```
# Start playground
docker run -it --pull=always -p 4566:4566 -p 5691:5691 ghcr.io/risingwavelabs/risingwave:latest playground
```

### standalone minio
To start a RisingWave standalone mode with minio backend, run

```
# Start all components
docker-compose up
```

### distributed cluster minio
To start a RisingWave cluster with minio backend, run

```
# Start all components
docker-compose -f docker-compose-distributed.yml up
```

It will start a minio, a meta node, a compute node, a frontend, a compactor, a prometheus and a redpanda instance.

### s3 and other s3-compatible storage backend
To start a RisingWave cluster with s3 backend, configure the aws credit in [aws.env](https://github.com/risingwavelabs/risingwave/blob/main/docker/aws.env).
If you want to use some s3 compatible storage like Tencent Cloud COS, just configure one more [endpoint](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/aws.env#L7).
After configuring the environment and fill in your [bucket name](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-s3.yml#L196), run

```
# Start all components
docker-compose -f docker-compose-with-s3.yml up
```

It will run with s3 (compatible) object storage with a meta node, a compute node, a frontend, a compactor, a prometheus and a redpanda instance.

### Start with other storage products of  public cloud vendors
To start a RisingWave cluster with other storage backend, like Google Cloud Storage, Alicloud OSS or Azure Blob Storage, configure the authentication information in [multiple_object_storage.env](https://github.com/risingwavelabs/risingwave/blob/main/docker/multiple_object_storage.env), fill in your [bucket name](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-gcs.yml#L196).
and run

```
# Start all components
docker-compose -f docker-compose-with-xxx.yml up
```

It will run RisingWave with corresponding (object) storage products.

### Start with HDFS backend
To start a RisingWave cluster with HDFS, mount your `HADDOP_HOME` in [compactor node volumes](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-hdfs.yml#L28), [compute node volumes](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-hdfs.yml#L112) [compute node volumes](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-hdfs.yml#L218), fill in the [cluster_name/namenode](https://github.com/risingwavelabs/risingwave/blob/a2684461e379ce73f8d730982147439e2379de16/docker/docker-compose-with-hdfs.yml#L202),
and run

```
# Start all components
docker-compose -f docker-compose-with-hdfs.yml up
```

It will run RisingWave with HDFS.

To clean all data, run:

```
docker-compose down -v
```

> [!NOTE]
>
> For RisingWave kernel hackers, we always recommend using [risedev](../src/risedevtool/README.md) to start the full cluster, instead of using docker images.
> See [CONTRIBUTING](../CONTRIBUTING.md) for more information.

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
