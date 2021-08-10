import os
import sys
import subprocess
return_code = 0
for file in os.listdir("./e2e_test_java/"):
    if file.endswith(".slt"):
        proc = subprocess.run([os.path.abspath("./cpp/third_party/output/bin/sqllogictest"), 
                                                       "-port", "4567", "-file", "e2e_test_java/" + file, "-pgdb", "dev"])
        if proc.returncode != 0:
            return_code = 1

sys.exit(return_code)