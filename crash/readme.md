# Crash image 


## Building crash image

Build the crash image via

```bash
docker build . -t crash

# Check that new image is present
docker image ls | head -2 
# REPOSITORY             TAG                  IMAGE ID       CREATED         SIZE
# crash                  latest               353ffa759902   4 minutes ago   892MB

docker run crash 
# You should see a few random numbers and then a panic
```

# Setting up local kind cluster

Setup cluster and connect against it

```bash 
onebox d
onebox setup

# Select the `kind-onebox` kubectl context
kubectl cluster-info # Should show local cluster, e.g. 
# Kubernetes control plane is running at https://0.0.0.0:55418
```

Load your docker image into your cluster. I assume that your kind cluster is called `onebox`

```bash
kind load docker-image crash --name onebox

# Ensure that the image is actually present on your kind notes 
for node in $(docker ps | grep onebox | awk '{print $NF}') ; do echo "on node $node" ; docker exec -it $node crictl images | grep crash ; done
# Output should look something like this
# on node onebox-worker
# docker.io/library/crash                                     latest               353ffa7599027       904MB
# on node onebox-control-plane
# docker.io/library/crash                                     latest               353ffa7599027       904MB
```

At this point we have a running dev cluster with our crash image. We can now use the crash image in deployments inside our cluster


# Setup our crashing application 

```bash 
kubectl apply -f k8s_resources/crash_deployment.yaml
watch kubectl get pods -A -o wide
# You should see the crash pods being created and restarting every few seconds and then
```



