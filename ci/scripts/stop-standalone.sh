#!/usr/bin/env bash

set -euo pipefail

pkill standalone
cargo make ci-kill
