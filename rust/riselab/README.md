# RiseLAB: The Ultimate Developer Tool

What is RiseLAB?
- When developing -- it is a playground that automatically builds and bootstraps all components.
- When deploying -- it is a config generator.
- For end users -- it can start a minimum playground environment.

This document will introduce RiseLAB from two perspectives: usage-level and dev-level, which cover all necessary information to use and develop RiseLAB.

## Usage-Level Guide

As a RisingWave developer, you may need to start components and debug components. RiseLAB makes your life easier with a flexible scene configuration and a service bootstrapper.

### Service Bootstrapper

#### Run e2e test

Simply run:

```bash
./riselab d # Default dev mode
./riselab <other modes>
./riselab k # Kill cluster
```

The default scene contains a MinIO, prometheus, compute-node, meta-node and a frontend. RiseLAB will automatically download and configure those services for you.

RiseLAB also provides several other modes:
- ci-3node: 3 compute node + meta node + frontend (no MinIO)
- ci-1node: 1 compute node + meta node + frontend (no MinIO)
- dev-compute-node: 1 compute-node (user managed) + MinIO + prometheus + meta + frontend

#### Debug compute node

Sometimes, you might want to debug a single component, but need to spawn all other components to make that component work. For example, debugging the compute node. In this case, simply run:

```bash
./riselab dev-compute-node
```

And you will see:

```plain
✅ tmux: session riselab
✅ minio: api http://127.0.0.1:9301/, console http://127.0.0.1:9400/
.. compute-node-5688: waiting for user-managed service online... (you should start it!)
.. playground: starting 5 services for dev-compute-node...
```

Then, you need simply start compute-node by yourself -- either in command line by cargo run or use debuggers such as CLion to start this component.

### Configuration File

`riselab.yml` defines all available scenes. Let's take a look at the ci-3node config:

```yaml
riselab:
  ci-3node:
    - use: compute-node
      port: 5687
      exporter-port: 1222
    - use: compute-node
      port: 5688
      exporter-port: 1223
    - use: compute-node
      port: 5689
      exporter-port: 1224
    - use: meta-node
    - use: frontend
```

The RiseLAB playground will start these 5 services in sequence. The service type is set with `use`. `port: 5687` overrides the default config for compute-node.
If you don't want one service, or want them to start in different order, simply remove or switch them. For example, if we only need two compute nodes:

```yaml
riselab:
  ci-3node:
    - use: compute-node
      port: 5687
      exporter-port: 1222
    - use: compute-node
      port: 5688
      exporter-port: 1223
    - use: meta-node
    - use: frontend
```

If you don't want to download some components, you may use the interactive configuration tool `./riselab configure` to disable some components.

That's all! RiseLAB will generate a new `server.properties` for the frontend node, and everything will be fine.

### Logs and Artifacts

- All artifacts of RiseLAB will be stored in .risingwave folder.
- The log folder contains log of all components.
- RiseLAB uses tmux to manage all components. Use tmux a to attach to the tmux session, and you will find all components running in the background.

## Dev-Level Guide

### Component Preparation

RiseLAB uses cargo-make to prepare and download all necessary components. Upon the first time of starting riselab, the config wizard will ask for components to download. The default dev environment requires all components to be installed.
The configurator will write an env file to riselab-components.user.env.

```bash
RISELAB_CONFIGURED=true
ENABLE_MINIO=true
ENABLE_BUILD_RUST=true
ENABLE_BUILD_FRONTEND=true
```

This environment file will then be read by cargo-make, which decides whether or not to run a step.

```
[cargo-make] INFO - Skipping Task: check-riselab-configured 
[cargo-make] INFO - Running Task: download-minio
[cargo-make] INFO - Running Task: download-mcli
[cargo-make] INFO - Skipping Task: download-grafana 
[cargo-make] INFO - Skipping Task: download-prometheus 
[cargo-make] INFO - Running Task: build-risingwave
```

As `ENABLE_PROMETHEUS_GRAFANA` is not set, download-grafana step is skipped.
All steps for downloading components, copying config, and building RisingWave are described as cargo-make's toml config. See `riselab/*.toml` and `Makefile.toml` for more information.

### Config Expander

`riselab.yml` is powerful yet simple. If you want to make changes to the configuration format, you may need to understand how it works. Source code is in `riselab/src/config`.

#### Template Expanding

`riselab.yml` has two sections: template and riselab. The template section contains templates for a single component. For example:

```yaml
template:
  compute-node:
    address: "127.0.0.1"
    port: 5688
    exporter-address: "127.0.0.1"
    exporter-port: 1222
    id: compute-node-${port}
    provide-minio: "minio*"
    user-managed: false

riselab:
  ci-3node:
    - use: compute-node
      port: 5687
      exporter-port: 1222
```

riselab will expand this config into:

```yaml
template:
  compute-node:
    address: "127.0.0.1"
    port: 5688
    exporter-address: "127.0.0.1"
    exporter-port: 1222
    id: compute-node-${port}
    provide-minio: "minio*"
    user-managed: false

riselab:
  ci-3node:
    - use: compute-node
      address: "127.0.0.1"
      exporter-address: "127.0.0.1"
      id: compute-node-${port}
      provide-minio: "minio*"
      user-managed: false
      port: 5687
      exporter-port: 1222
```

The config in template (namely port and exporter port) will be overwritten by user-provided config.

#### Variable Expanding

In ci-3node config,

```yaml
ci-3node:
  - use: compute-node
    address: "127.0.0.1"
    exporter-address: "127.0.0.1"
    id: compute-node-${port}
    provide-minio: "minio*"
    user-managed: false
    port: 5687
    exporter-port: 1222
```
`${port}` will be expanded to `5687`, the field of the same name in the current yaml map.

```yaml
ci-3node:
  - use: compute-node
    address: "127.0.0.1"
    exporter-address: "127.0.0.1"
    id: compute-node-5687
    provide-minio: "minio*"
    user-managed: false
    port: 5687
    exporter-port: 1222
```

#### Wildcard Expanding

The `*` will be expanded by id. For example, in this frontend config, riselab will find all ids that matches compute-node* in the current scene.

```yaml
  frontend:
    address: "127.0.0.1"
    port: 4567
    id: frontend
    provide-compute-node: "compute-node*"
    provide-meta-node: "meta-node*"
    user-managed: false
```

For ci-3node, there are three compute nodes, so this will be expanded into:

```yaml
 -  use: frontend
    address: "127.0.0.1"
    port: 4567
    id: frontend
    provide-compute-node: ["compute-node-5687", "compute-node-5688", "compute-node-5689"]
    provide-meta-node: ["meta-node-5690"]
    user-managed: false
```

#### Component Provision

All `provide-` items will be expanded to their corresponding components by id matching. For example, in this case, RiseLAB will find the config for `compute-node-5687`, etc., and copy them into the config of frontend:

```yaml
- address: 127.0.0.1
  port: 4567
  id: frontend
  provide-compute-node:
    - address: 127.0.0.1
      exporter-address: 127.0.0.1
      id: compute-node-5687
      user-managed: false
      use: compute-node
      port: 5687
      exporter-port: 1222
    - address: 127.0.0.1
      exporter-address: 127.0.0.1
      id: compute-node-5688
      user-managed: false
      use: compute-node
      port: 5688
      exporter-port: 1223
    - address: 127.0.0.1
      exporter-address: 127.0.0.1
      id: compute-node-5689
      user-managed: false
      use: compute-node
      port: 5689
      exporter-port: 1224
```

This expanded config will serve as a base config for the following config generator component.

### Config Generator

RiseLAB will generate config of each service using modules in riselab/src/config_gen. Given the above frontend meta-config, the server.properties will be generated:

```apache
risingwave.pgserver.ip=127.0.0.1
risingwave.pgserver.port=4567
risingwave.leader.clustermode=Distributed
risingwave.leader.computenodes=127.0.0.1:5687,127.0.0.1:5688,127.0.0.1:5689

risingwave.catalog.mode=Remote
risingwave.meta.node=127.0.0.1:5690
```

### RiseLAB Service

The RiseLAB playground will read the config and start all the services in sequence. The tasks will be started in tmux. All commands run by playground can be found in `riselab.log` in `.risingwave/log`. After starting each service, it will check liveness and return code of the program, so as to ensure a service is running.

These components conclude the internal implementation of RiseLAB.
