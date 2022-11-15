.EXPORT_ALL_VARIABLES:
PROTOC = /bin/protoc
DOCKER_BUILDKIT = 1

# creates the build container
build-devcont: 
	docker build -t rwk8scont -f k8s_workflow/Dockerfile_base .

# builds all risingwave containers
build-docker:
	mkdir -p docker_target
	mkdir -p docker_bin
	docker run -v ${PWD}/src:/risingwave/src -v ${PWD}/docker_bin:/risingwave/bin -v ${PWD}/docker_target:/risingwave/target -v ${PWD}/k8s_workflow:/risingwave/k8s_workflow --workdir /risingwave rwk8scont /bin/bash -c "./k8s_workflow/compile.sh"
	./k8s_workflow/build.sh

k8s-frontend-node: 
	bash -c "docker save frontend-node | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	k -n rw-2-mytenant set image deployment/risingwave-compactor
	kubectl -n rwc-2-mytenant delete --force -l risingwave/component=frontend
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=frontend

k8s-compactor-node: 
	bash -c "docker save compactor-node | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	kubectl -n rwc-2-mytenant delete --force -l risingwave/component=compactor
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=compactor

k8s-meta-node:
	bash -c "docker save meta-node | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	kubectl -n rwc-2-mytenant delete --force -l risingwave/component=meta
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=meta

k8s-compute-node:
	bash -c "docker save compute-node | docker exec --privileged -i onebox-control-plane ctr --namespace=k8s.io images import --all-platforms -"
	kubectl -n rwc-2-mytenant delete --force -l risingwave/component=compute
	kubectl -n rwc-2-mytenant wait --for=condition=ready pod -l risingwave/component=compute

# Is this the image for everything?
k8s-risingwave:
	exit 1

# update all rw components and wait for ready status
k8s-all: build-docker
	

# run this in case you run into "docker: no more space on device"
prune: 
	docker system prune


