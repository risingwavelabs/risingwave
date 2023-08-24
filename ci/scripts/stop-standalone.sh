#!/usr/bin/env bash

set -euo pipefail

pkill risingwave
cargo make ci-kill
