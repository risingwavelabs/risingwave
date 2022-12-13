#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

bash "${DIR}/test_basic.sh"
bash "${DIR}/test_pin_sst.sh"
