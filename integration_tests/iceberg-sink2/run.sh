#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

curl -sSL https://install.python-poetry.org | python3 -

# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
"$HOME"/.local/bin/poetry update --quiet
"$HOME"/.local/bin/poetry run python main.py