# Docker Images

RisingWave currently *only supports Linux x86_64* for building docker images.

To build the images, simply run:

```
make docker_frontend
make docker_backend
```

in the project root. It takes dozons of minutes to finish building.

For RisingWave kernel hackers, we always recommend using [risedev](../src/risedevtool/README.md) to start the full cluster, instead of using docker images.
See [CONTRIBUTING](../CONTRIBUTING.md) for more information.

If you are interested in experiencing resingwave with Docker and without installing all the dependencies. You can refer to the following commands. It will have you set up a Docker-based cluster, which contains one minio server, one metanode, one computenode and one frontend. Please run these commands in the project root. Currently, only *only Linux x86_64* is supported.

```
# Run minio server
docker run -d --name minio_container --net=host -e "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" -e "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" quay.io/minio/minio server /data --console-address ":9001"

# Get into the minio client interactive shell 
docker run --net=host -it --entrypoint=/bin/sh minio/mc

# Run these commands in the interactive shell to setup buckets
mc alias set hummock-minio http://127.0.0.1:9000 AKIAIOSFODNN7EXAMPLE wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
mc admin user add hummock-minio/ hummock 12345678
mc admin policy set hummock-minio/ readwrite user=hummock
mc mb hummock-minio/hummock001
exit 

# Run metanode
docker run -d --name metanode_container --net=host risingwave/backend:latest /bin/bash -c /risingwave/bin/meta-node --host 127.0.0.1:5690 --dashboard-host 127.0.0.1:5691 --prometheus-host 127.0.0.1:1250

# Run computenode
docker run -d --name computenode_container --net=host -v $(pwd)/src/config:/risingwave/config risingwave/backend:latest /bin/bash -c /risingwave/bin/compute-node --config-path /risingwave/config/risingwave.toml --host 127.0.0.1:5688 --prometheus-listener-addr 127.0.0.1:1222 --metrics-level 1 --state-store hummock+minio://hummock:12345678@127.0.0.1:9000/hummock001 --meta-address http://127.0.0.1:5690

# Run frontend, if you have built its image with make docker_frontend
docker run -d --name frontend_container --net=host risingwave/frontend:latest /bin/bash -c /risingwave/bin/frontend-v2 --host 127.0.0.1:4566 --meta-addr http://127.0.0.1:5690

# Run frontend_lagecy, only if you have built its image with make docker_frontend_lagecy
# docker run -d --name frontend_container --net=host risingwave/frontend:latest /bin/bash -c 'sed -i "1d" conf/server.properties; echo "risingwave.pgserver.ip=127.0.0.1" >> conf/server.properties; java -cp lib/risingwave-fe-runnable.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005 -Dlogback.configurationFile=conf/logback.xml com.risingwave.pgserver.FrontendServer -c conf/server.properties'

# Connect to risingwave server
psql -h localhost -p 4566 -d dev

# If using frontend_lagecy
psql -h localhost -p 4567 -d dev
```

We will add another way to set up a cluster based on docker-compose in the near future.