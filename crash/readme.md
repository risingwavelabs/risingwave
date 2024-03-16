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

cd capture 
docker build . -t capture
docker image ls | head -2 
```

# Setting up local kind cluster

Setup cluster and connect against it

```bash 
onebox d
onebox setup kind -n 2

# Select the `kind-onebox` kubectl context
kubectl cluster-info # Should show local cluster, e.g. 
# Kubernetes control plane is running at https://0.0.0.0:55418
```

Load your docker images into your cluster. I assume that your kind cluster is called `onebox`

```bash
kind load docker-image crash --name onebox
# Ensure that the crash image is actually present on your kind notes 
for node in $(docker ps | grep onebox | awk '{print $NF}') ; do echo "on node $node" ; docker exec -it $node crictl images | grep crash ; done
# Output should look something like this
# on node onebox-worker
# docker.io/library/crash             latest               353ffa7599027       904MB
# on node onebox-control-plane
# docker.io/library/crash             latest               353ffa7599027       904MB

# repeat for the capture image
kind load docker-image capture --name onebox
for node in $(docker ps | grep onebox | awk '{print $NF}') ; do echo "on node $node" ; docker exec -it $node crictl images | grep capture ; done
```

At this point we have a running dev cluster with our crash image. We can now use the crash image in deployments inside our cluster


# Setup our crashing application 

```bash 
kubectl apply -f k8s_resources/crash_deployment.yaml
watch kubectl get pods -A -o wide
# You should see the crash pods being created and restarting every few seconds
# You may observe that the crash pods are located on different worker nodes
```

# Setting up capture infrastructure 

```bash 
k apply -f k8s_resources/capture_ds.yaml 
```

# Obverse capturing 

```bash
k get pods -A -o wide 

# follow one of the crashing applications 
k logs crash-deployment-56b9bcb7b-4nsxr --follow

# follow one of the capturers, make sure it is on the same node as the crashing applicaiton
k logs  capture-b9m29 --follow
```

You should see how core dumps are created in your application

```log
thread 'main' panicked at 'randomly panicking here', src/main.rs:24:13
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
./entrypoint.sh: line 7:    12 Quit                    (core dumped) /crash/target/debug/crash
```

These core dumps should then be captured

```log
[2023-07-12 14:09:16] [coredump] 'core.crash.sig3.11.1689170956' has been uploaded to 's3://myorg-coredump/__CLUSTER_NAME__``/onebox-worker.core.crash.sig3.11.1689170956'
```