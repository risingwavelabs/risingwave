#!/bin/bash
set -ex

docker compose up -d
sleep 10

cd java || exit

sudo yum install -y java-11-openjdk-devel epel-release maven

# Verify the Java installation
java -version
mvn -version
# Run Java tests
mvn clean test || docker ps -a --format "{{.ID}}" | xargs -I {} docker logs {}

cd ../python || exit
sudo yum update -y
sudo yum install -y python3
pip3 install -r requirements.txt
export PATH=$PATH:$HOME/.local/bin
pytest

cd ../go || exit
sudo yum install gcc -y
wget https://dl.google.com/go/go1.21.1.linux-amd64.tar.gz
sudo tar -C /usr/local -xf go1.21.1.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
sh run.sh


echo "--- clean docker compose"
docker compose down --remove-orphans -v
