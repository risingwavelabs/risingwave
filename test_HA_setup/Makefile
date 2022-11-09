.EXPORT_ALL_VARIABLES:
PROTOC = /usr/bin/protoc

# Only execute this in the devcontainer
build:
	~/.cargo/bin/cargo build

run-local: build
	./target/debug/simple_web

build-devcont: 
	docker build -t rustdevcont -f Dockerfile_devcont .

# docker run -v $(pwd):/usr/simple_web -it rustdevcont /bin/bash
# Docker container has to contain make and rust toolchain
build-docker:
	docker run -v /Users/janmensch/Documents/github/test_HA_setup:/usr/simple_web --workdir /usr/simple_web rustdevcont /bin/bash -c "make build"
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
	kubectl wait --for=condition=ready pod -l app=simpleweb
	k -n default port-forward service/simpleweb-service 8080:8080

prune: 
	docker system prune -y
	docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io content prune references


