#!/usr/bin/env python3
import json
# Executes full benchmark for stream_hash_join runtime and memory consumption
# Outputs a json file with the results.

import subprocess
import re
import sys

# Print header
results = ["Amp,Workload,JoinType,Total Blocks,Total Bytes,Runtime (ns)"]

# Run benchmarks and capture results
for amp in [20_000, 40_000, 200_000, 400_000]:
    for workload in ["NotInCache", "InCache"]:
        for join_type in ["Inner", "LeftOuter"]:
            # Construct the command
            cmd_mem = f'ARGS={amp},{workload},{join_type} cargo bench --features dhat-heap --bench stream_hash_join_mem'
            cmd_rt = f'cargo criterion --message-format json --bench stream_hash_join_rt -- hash_join_rt_{amp}_{workload}_{join_type}'

            s = ""

            try:
                # Run cmd_mem and capture output
                output = subprocess.check_output(cmd_mem, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)

                # Extract total blocks and bytes
                total_blocks_match = re.search(r'max_blocks:\s*(\d+)', output)
                total_bytes_match = re.search(r'max_bytes:\s*(\d+)', output)

                if total_blocks_match and total_bytes_match:
                    total_blocks = total_blocks_match.group(1)
                    total_bytes = total_bytes_match.group(1)

                    s+=f"{amp},{workload},{join_type},{total_blocks},{total_bytes}"
                else:
                    print(f"No total_blocks or total_bytes found for: Amp={amp}, Workload={workload}, JoinType={join_type}", file=sys.stderr)

                # Run cmd_rt and capture output
                json_output = subprocess.check_output(cmd_rt, shell=True, universal_newlines=True)
                json_output = json_output.split('\n')
                try:
                    time_ns = json.loads(json_output[0])["typical"]["estimate"]
                except Exception as e:
                    print(f"could not parse {json_output[0]} due to {e}")
                    exit(1)
                if time_ns:
                    s+=f",{time_ns}"
                else:
                    print(f"No runtime found for: Amp={amp}, Workload={workload}, JoinType={join_type}", file=sys.stderr)

                results.append(s)

            except subprocess.CalledProcessError as e:
                print(f"Error running benchmark for Amp={amp}, Workload={workload}, JoinType={join_type}", file=sys.stderr)
                print(f"Error output: {e.output}", file=sys.stderr)

for result in results:
    print(result)