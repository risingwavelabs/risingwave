#!/usr/bin/python3

import argparse
import os
import sys
from os.path import (dirname, abspath)


file_server = """  file_server:
    image: halverneus/static-file-server:latest
    volumes:
      - "./:/demo"
    restart: always
    environment:
      FOLDER: /demo
    container_name: file_server
"""


def gen_docker_compose(demo_compose: str, format: str):
    content = ""
    with open(demo_compose) as file:
        for line in file:
            line = line.replace("      - /datagen",
                                "      - /datagen --format {}".format(format))
            if line == 'volumes:\n':
                content += file_server
            content += line
    with open(demo_compose, 'w') as file:
        file.write(content)


demo = sys.argv[1]
if demo == 'docker':
    print('Will not generate docker-compose file for `docker`')
    sys.exit(0)

format = sys.argv[2]
if format not in ["json", "protobuf", "avro"]:
    print('Invalid format: {}'.format(format))
    sys.exit(1)

file_dir = dirname(abspath(__file__))
project_dir = dirname(file_dir)
demo_dir = os.path.join(project_dir, demo)
demo_compose = os.path.join(demo_dir, 'docker-compose.yml')

gen_docker_compose(demo_compose, format)
