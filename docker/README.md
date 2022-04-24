# Docker Images

RisingWave currently *only supports Linux x86_64* for building docker images.

To build the images, simply run:

```
make docker
```

in the project root.

To start entire app, run

```
make docker-compose
```

in the project root. It will start a minio, a meta node, a compute node, a frontend and a redpanda instance.

For RisingWave kernel hackers, we always recommend using [risedev](../src/risedevtool/README.md) to start the full cluster, instead of using docker images.
See [CONTRIBUTING](../CONTRIBUTING.md) for more information.
