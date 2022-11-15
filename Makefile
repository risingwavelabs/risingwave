.EXPORT_ALL_VARIABLES:
PROTOC = /bin/protoc

# Only execute this in the devcontainer
build:
	~/.cargo/bin/cargo build

run-local: build
	./target/debug/simple_web

build-devcont: 
	docker build -t rwk8scont -f k8s_workflow/Dockerfile_base .

build-docker:
	mkdir -p docker_target
	mkdir -p docker_bin
	docker run -v ${PWD}/src:/risingwave/src -v ${PWD}/docker_bin:/risingwave/bin -v ${PWD}/docker_target:/risingwave/target -v ${PWD}/k8s_workflow:/risingwave/k8s_workflow --workdir /risingwave rwk8scont /bin/bash -c "./k8s_workflow/compile.sh"
	./k8s_workflow/build.sh


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
	docker system prune
	docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io content prune references


# TODO: build meta node 

