#!/usr/bin/env bash

set -euo pipefail

cat -vt $1 \
 | LC_ALL=C LANG=C sed -E \
  -e 's/\^\[_bk\;t=[0-9]+//g' \
  -e 's/\^[A-Z]//g' \
  -e 's/\^\[\[[0-9A-Z]+m{0,1}//g'