.EXPORT_ALL_VARIABLES:
PROTOC = /bin/protoc
DOCKER_BUILDKIT = 1

# creates the build container
build-devcont: 
	docker build -t rwk8scont -f k8s_workflow/Dockerfile_base .

# You may need to change the memory settings in Docker Desktop > Settings > Resources > Advanced
# builds all risingwave containers
build-docker:
	mkdir -p docker_target
	mkdir -p docker_bin
	docker run --memory=13g --cpus="7" -v ${PWD}/src:/risingwave/src -v ${PWD}/docker_bin:/risingwave/bin -v ${PWD}/docker_target:/risingwave/target -v ${PWD}/k8s_workflow:/risingwave/k8s_workflow --workdir /risingwave rwk8scont /bin/bash -c "./k8s_workflow/compile.sh"
	./k8s_workflow/build.sh

k8s-frontend-node: build-docker
	bash -c "docker save risingwave | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	k -n rwc-2-mytenant set image deployment/risingwave-frontend frontend=risingwave
	kubectl -n rwc-2-mytenant delete pod --force -l risingwave/component=frontend
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=frontend

k8s-compactor-node: build-docker
	bash -c "docker save risingwave | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	k -n rwc-2-mytenant set image deployment/risingwave-compactor compactor=risingwave
	kubectl -n rwc-2-mytenant delete pod --force -l risingwave/component=compactor
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=compactor

k8s-meta-node: build-docker
	bash -c "docker save risingwave | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	k -n rwc-2-mytenant set image deployment/risingwave-meta meta=risingwave
	kubectl -n rwc-2-mytenant delete pod --force -l risingwave/component=meta
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=meta

# compute does not use deployment
k8s-compute-node: build-docker
	exit 1
	bash -c "docker save risingwave | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	k -n rwc-2-mytenant set image pod/risingwave-compactor-o compute=risingwave
	kubectl -n rwc-2-mytenant delete pod --force -l risingwave/component=compute
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=compute

# update all rw components and wait for ready status
# does not update k8s-compute-node
k8s-all: k8s-meta-node k8s-compactor-node k8s-frontend-node

# run this in case you run into "docker: no more space on device"
prune: 
	docker system prune

# remove docker build directories
clean: 
	sudo rm -rf docker_bin
	sudo rm -rf docker_target
	mkdir docker_bin
	mkdir docker_target

# If you get "Blocking waiting for file lock on build directory", please delete all other containers that still try to compile 
