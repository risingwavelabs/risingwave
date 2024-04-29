#!/bin/bash
for i in {0..9}; do
cat <<EOF | rpk topic produce shared_source -f "%p %v\\n" -p 0
0 {"v1": 1, "v2": "a"}
1 {"v1": 2, "v2": "b"}
2 {"v1": 3, "v2": "c"}
3 {"v1": 4, "v2": "d"}
EOF
done