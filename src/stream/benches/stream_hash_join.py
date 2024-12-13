#!/usr/bin/env python3

# Executes full benchmark for stream_hash_join runtime and memory consumption
# Outputs a json file with the results.

import subprocess
import re
import sys

# Print header
print("Amp,Workload,JoinType,Total Blocks,Total Bytes")

# Run benchmarks and capture results
for amp in [10_000, 20_000, 30_000, 40_000, 100_000, 200_000, 400_000]:
    for workload in ["NotInCache", "InCache"]:
        for join_type in ["Inner", "LeftOuter"]:
            # Construct the command
            cmd = f'ARGS={amp},{workload},{join_type} cargo bench --features dhat-heap --bench stream_hash_join_mem'

            # Run the command and capture output
            try:
                output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)

                # Extract total blocks and bytes
                total_blocks_match = re.search(r'max_blocks:\s*(\d+)', output)
                total_bytes_match = re.search(r'max_bytes:\s*(\d+)', output)

                if total_blocks_match and total_bytes_match:
                    total_blocks = total_blocks_match.group(1)
                    total_bytes = total_bytes_match.group(1)

                    # Print results immediately
                    print(f"{amp},{workload},{join_type},{total_blocks},{total_bytes}")
                else:
                    print(f"No total_blocks or total_bytes found for: Amp={amp}, Workload={workload}, JoinType={join_type}", file=sys.stderr)

            except subprocess.CalledProcessError as e:
                print(f"Error running benchmark for Amp={amp}, Workload={workload}, JoinType={join_type}", file=sys.stderr)
                print(f"Error output: {e.output}", file=sys.stderr)