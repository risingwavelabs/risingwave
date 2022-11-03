.EXPORT_ALL_VARIABLES:
PROTOC = /usr/bin/protoc

# Only execute this in the devcontainer
build:
	sudo apt update
	sudo apt install -y protobuf-compiler
	cargo build

run-local: build
	./target/debug/simple_web

build-docker:
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
	k delete --force -f simpleweb.yaml
	k apply -f simpleweb.yaml
	k port-forward service/simpleweb-service 8080:8080



