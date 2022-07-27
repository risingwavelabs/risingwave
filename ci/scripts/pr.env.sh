#!/usr/bin/env bash
set -euo pipefail

# Set features, depending on our workflow
# If sqlsmith files are modified, we run unit tests with sqlsmith enabled
# by overriding RUSTFLAGS to enable sqlsmith feature.
export RUSTFLAGS="$(RUSTFLAGS) --cfg enable_sqlsmith_unit_test"
# Otherwise we use default.
