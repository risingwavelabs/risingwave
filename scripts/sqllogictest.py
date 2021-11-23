import os
import sys
import subprocess
import argparse
import glob

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", default="4567")
parser.add_argument("-db", "--pgdb", default="postgres")
parser.add_argument("-u", "--user", default="postgres")
parser.add_argument("-f", "--files", default="./e2e_test/distributed/")
# Note we do not need pass word yet.
args = parser.parse_args()
return_code = 0

files = glob.glob(os.path.join(args.files, "**/*.slt"), recursive=True)
for file in files:
    proc = subprocess.run([os.path.abspath("./go/bin/sqllogictest"),
                           "-port", args.port, "-file", file, "-pgdb", args.pgdb])
    if proc.returncode != 0:
        return_code = 1
        break

sys.exit(return_code)
