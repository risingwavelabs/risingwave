#!/usr/bin/python3

from os.path import (dirname, abspath)
import os
import subprocess
import argparse


def clean_demo(demo: str):
    file_dir = dirname(abspath(__file__))
    project_dir = dirname(file_dir)
    demo_dir = os.path.join(project_dir, demo)
    print("Clean demo: {}".format(demo))

    subprocess.run(["docker", "compose", "down"], cwd=demo_dir, check=True)

arg_parser = argparse.ArgumentParser(description='Clean the demo')
arg_parser.add_argument('--case',
                        metavar='case',
                        type=str,
                        help='the test case')
args = arg_parser.parse_args()

clean_demo(args.case)
