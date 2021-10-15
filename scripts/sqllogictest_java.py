import os
import sys
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port")
parser.add_argument("-db", "--pgdb", default="postgres")
parser.add_argument("-u", "--user", default="postgres")
# Note we do not need pass word yet.
args = parser.parse_args()
return_code = 0
for file in os.listdir("./e2e_test_java/"):
    if file.endswith(".slt"):
        proc = subprocess.run([os.path.abspath("./go/bin/sqllogictest"),
                                                       "-port", args.port, "-file", "e2e_test_java/" + file, "-pgdb", args.pgdb])
        if proc.returncode != 0:
            return_code = 1
            break
        
sys.exit(return_code)
