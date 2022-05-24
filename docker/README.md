# Docker Images

RisingWave currently *only supports Linux x86_64* for building docker images.

To build the images, simply run:

```
make docker
```

in the project root.

To ensure you are using the latest version of RisingWave image,

```
# Ensure risingwave image is of latest version
docker pull ghcr.io/singularity-data/risingwave:latest
```

To start a RisingWave playground, run

```
# Start playground
docker run -it ghcr.io/singularity-data/risingwave:latest
```

To start a RisingWave cluster, run

```
# Start all components
docker-compose up
```

It will start a minio, a meta node, a compute node, a frontend, a compactor, a prometheus and a redpanda instance.

To clean all data, run:

```
docker-compose down -v
```

For RisingWave kernel hackers, we always recommend using [risedev](../src/risedevtool/README.md) to start the full cluster, instead of using docker images.
See [CONTRIBUTING](../CONTRIBUTING.md) for more information.

# Generate docker-compose.yml

```bash
./risedev compose --single-file
```

# Deploy with RiseDev Compose

If you want to deploy with docker-compose without using something like docker swarm, we can use host network mode and separately start components on different servers.

Firstly, you'll need 5 servers with 5 host names: `rw-source`, `rw-meta`, `rw-compute-1`, `rw-compute-2` and `rw-compute-3`. The hostnames will need to be resolved into IP by DNS or `/etc/hosts`. After that, run the compose command:

```bash
./risedev compose compose-3node-host-deploy --host-mode
```

In the docker directory you'll find 5 docker-compose files, to be deployed at 5 servers separately.
