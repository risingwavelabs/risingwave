#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
poetry update --quiet
poetry run python main.py