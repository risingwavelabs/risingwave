#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Install poetry

export POETRY_HOME=/opt/poetry
python3 -m venv $POETRY_HOME
$POETRY_HOME/bin/pip install poetry==1.8.0
$POETRY_HOME/bin/poetry --version

# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
$POETRY_HOME/bin/poetry update --quiet
$POETRY_HOME/bin/poetry run python main.py