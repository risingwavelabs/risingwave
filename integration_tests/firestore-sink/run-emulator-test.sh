#!/usr/bin/env bash
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail

cd "$(dirname "$0")/../.."
test_dir=$(mktemp -d)
emulator_pid=
cleanup() {
  if [[ -n "$emulator_pid" ]]; then
    kill "$emulator_pid" 2>/dev/null || true
    wait "$emulator_pid" 2>/dev/null || true
  fi
  rm -rf "$test_dir"
}
trap cleanup EXIT

emulator_jar=${FIRESTORE_EMULATOR_JAR:-$test_dir/firestore-emulator.jar}
emulator_port=${FIRESTORE_EMULATOR_PORT:-18080}
if [[ ! -f "$emulator_jar" ]]; then
  curl --fail --location --retry 3 \
    https://storage.googleapis.com/firebase-preview-drop/emulator/cloud-firestore-emulator-v1.19.8.jar \
    --output "$emulator_jar"
fi
java -jar "$emulator_jar" --host 127.0.0.1 --port "$emulator_port" \
  --project_id demo-rw-firestore > "$test_dir/emulator.log" 2>&1 &
emulator_pid=$!
ready=false
for _ in $(seq 1 60); do
  if ! kill -0 "$emulator_pid" 2>/dev/null; then
    cat "$test_dir/emulator.log"
    exit 1
  fi
  if rg -q 'Dev App Server is now running' "$test_dir/emulator.log"; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  cat "$test_dir/emulator.log"
  exit 1
fi

FIRESTORE_EMULATOR_HOST="127.0.0.1:$emulator_port" \
  cargo test -p risingwave_connector --lib --no-default-features \
    sink::firestore::tests::emulator_crud_replay_and_native_values -- --ignored --nocapture
