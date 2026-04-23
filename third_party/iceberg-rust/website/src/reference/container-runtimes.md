<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Container Runtimes

Iceberg-rust uses containers for integration tests, where `docker` and `docker compose` start containers for MinIO and various catalogs. You can use any of the following container runtimes.

## Docker Desktop

[Docker Desktop](https://www.docker.com/products/docker-desktop/) is available for macOS, Windows, and Linux.

1. Install Docker Desktop by downloading the [installer](https://www.docker.com/products/docker-desktop/) or using Homebrew on macOS.
    ```shell
    brew install --cask docker
    ```

2. Launch Docker Desktop and complete the setup.

3. Verify the installation.
    ```shell
    docker --version
    docker compose version
    ```

4. Try some integration tests!
    ```shell
    make test
    ```

## OrbStack (macOS)

[OrbStack](https://orbstack.dev/) is a lightweight alternative to Docker Desktop on macOS.

1. Install OrbStack by downloading the [installer](https://orbstack.dev/download) or using Homebrew.
    ```shell
    brew install orbstack
    ```
   
2. Migrate Docker data (if switching from Docker Desktop).
    ```shell
    orb migrate docker
    ```
   
3. (Optional) Add registry mirrors.
   
    You can edit the config directly at `~/.orbstack/config/docker.json` and restart the engine with `orb restart docker`.
   
    ```json
    {
        "registry-mirrors": ["<mirror_addr>"]
    }
    ```

4. Try some integration tests!
    ```shell
    make test
    ```

## Podman

[Podman](https://podman.io/) is a daemonless container engine. The instructions below set up "rootful podman" with docker's official docker-compose plugin.

1. Have podman v4 or newer.
    ```shell
    $ podman --version
    podman version 4.9.4-rhel
    ```

2. Create a docker wrapper script:
   
    Create a fresh `/usr/bin/docker` file and add the below contents:
    ```bash
    #!/bin/sh
    [ -e /etc/containers/nodocker ] || \
    echo "Emulate Docker CLI using podman. Create /etc/containers/nodocker to quiet msg." >&2
    exec sudo /usr/bin/podman "$@"
    ```

    Set new `/usr/bin/docker` file to executable.
    ```shell
    sudo chmod +x /usr/bin/docker
    ```

3. Install the [docker compose plugin](https://docs.docker.com/compose/install/linux). Check for successful installation.
    ```shell
    $ docker compose version
    Docker Compose version v2.28.1
    ```

4. Append the below to `~/.bashrc` or equivalent shell config:
    ```bash
    export DOCKER_HOST=unix:///run/podman/podman.sock
    ```

5. Start the "rootful" podman socket.
    ```shell
    sudo systemctl start podman.socket
    sudo systemctl status podman.socket
    ```

6. Check that the following symlink exists.
    ```shell
    $ ls -al /var/run/docker.sock
    lrwxrwxrwx 1 root root 27 Jul 24 12:18 /var/run/docker.sock -> /var/run/podman/podman.sock
    ```
    If the symlink does not exist, create it.
    ```shell
    sudo ln -s /var/run/podman/podman.sock /var/run/docker.sock
    ```

7. Check that the docker socket is working.
    ```shell
    sudo curl -H "Content-Type: application/json" --unix-socket /var/run/docker.sock http://localhost/_ping
    ```

8. Try some integration tests!
    ```shell
    cargo test -p iceberg --test file_io_s3_test
    ```

### Note on rootless containers

As of podman v4, ["To be succinct and simple, when running rootless containers, the container itself does not have an IP address"](https://www.redhat.com/sysadmin/container-ip-address-podman). This causes issues with iceberg-rust's integration tests, which rely upon IP-addressable containers via docker-compose. As a result, podman "rootful" containers are required to ensure containers have IP addresses.

### Podman troubleshooting

**Error:** `short-name "apache/iceberg-rest-fixture" did not resolve to an alias and no unqualified-search registries are defined in "/etc/containers/registries.conf"`

**Fix:** Add or modify the `/etc/containers/registries.conf` file:
```toml
[[registry]]
prefix = "docker.io"
location = "docker.io"
```

### Podman references

* <https://docs.docker.com/compose/install/linux>
* <https://www.redhat.com/sysadmin/podman-docker-compose>
* <https://www.redhat.com/sysadmin/container-ip-address-podman>
* <https://github.com/containers/podman/blob/main/docs/tutorials/basic_networking.md>
