#!/bin/bash 

echo "in entrypoint.sh"
ulimit -c 1024000000
cat /proc/sys/kernel/core_pattern
mkdir -p /var/coredump
/crash/target/debug/crash

