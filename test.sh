#!/usr/bin/env bash

set -euo pipefail

./risedev d; ./risedev psql -f error.sql