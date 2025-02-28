#!/bin/bash

podman build -t ghcr.io/risingwavelabs/sink-bench:latest -f Dockerfile . --load
