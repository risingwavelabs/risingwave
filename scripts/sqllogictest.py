import os
import sys
import subprocess
return_code = 0
for file in os.listdir("./e2e_test/"):
    if file.endswith(".slt"):
        proc = subprocess.run([os.path.abspath("./cpp/third_party/output/bin/sqllogictest"), 
                                                       "-port", "12345", "-file", "e2e_test/" + file])
        if proc.returncode != 0:
            return_code = 1

sys.exit(return_code)

