# Docker Images

RisingWave currently *only supports Linux x86_64* for building docker images.

To build the images, simply run:

```
make docker_frontend
make docker_backend
```

in the project root.

For developers, we always recommend using risedev to start the full cluster, instead of using these docker images.
See [CONTRIBUTING](../CONTRIBUTING.md) for more information.
