.EXPORT_ALL_VARIABLES:
PROTOC = /usr/bin/protoc

# Only execute this in the devcontainer
build:
	sudo apt update
	sudo apt install -y protobuf-compiler
	cargo build

run-local: build
	./target/debug/simple_web

# Docker container has to contain make and rust toolchain
build-docker:
	docker run -v /Users/janmensch/Documents/simple_web:/usr/simple_web vsc-simple_web-bac13dc9507fbe01a8d16a12185ad7da-features /bin/bash -c "cd /usr/simple_web; make build"
	docker build -t simpleweb .

# Not sure why this does not work. Some mac issue
# docker network create my-network
run-docker: build-docker
	docker rm -f simpleweb
	docker run --network=my-network --name simpleweb -d -p 8080:8080 simpleweb 
	docker port simpleweb

# TODO: Load into kind
k8s: build-docker
	bash -c "docker save simpleweb | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	k delete --force -f simpleweb.yaml || true
	k apply -f simpleweb.yaml
	k -n kube-system port-forward service/simpleweb-service 8080:8080



