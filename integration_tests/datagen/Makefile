build-docker:  ## Build a docker container 
	docker build . -t ghcr.io/risingwavelabs/demo-datagen:latest

build-bin: ## Build the executable
	go build

run-bin: build-bin ## Run the executable
	./datagen --mode nested --qps 1000 kafka --brokers localhost:9092

run-all: 
	docker kill local_kafka || exit 0
	rm -rf /tmp/redpanda 
	mkdir /tmp/redpanda
	docker run --rm -d --name local_kafka -v /tmp/redpanda:/var/lib/redpanda/data -p 8081:8081 -p 8082:8082 -p 9092:9092 -p 9644:9644  docker.vectorized.io/vectorized/redpanda:latest redpanda start --smp 1 --reserve-memory 0M --memory 4G --overprovisioned --node-id 0 --check=false
	make run-bin 

# TODO: Add help target

