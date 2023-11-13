RED    = \033[0;31m
GREEN  = \033[0;32m
YELLOW = \033[0;33m
BLUE   = \033[0;36m
RESET  = \033[0m

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make ${BLUE}<target>${RESET}\n\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  ${BLUE}%-30s${RESET} %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s${RESET}\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build

build-docker:  ## Build a docker container 
	docker build . -t ghcr.io/risingwavelabs/demo-datagen:latest

build-bin: ## Build the executable
	go build

##@ Run

run-bin: build-bin ## Run the executable
	./datagen --mode nested --qps 1000 kafka --brokers localhost:9092

run-all: ## Run executable, risingwave, and kafka container
	docker kill local_kafka || exit 0
	docker kill rw || exit 0
	rm -rf /tmp/redpanda
	mkdir /tmp/redpanda
	docker run --rm -d --name local_kafka -v /tmp/redpanda:/var/lib/redpanda/data -p 8081:8081 -p 8082:8082 -p 9092:9092 -p 9644:9644  docker.vectorized.io/vectorized/redpanda:latest redpanda start --smp 1 --reserve-memory 0M --memory 4G --overprovisioned --node-id 0 --check=false
	docker run --rm -d --name rw -p 4566:4566 -p 5691:5691 risingwavelabs/risingwave:v1.4.0 playground
	@echo "connect via psql \"port=4566 host=localhost user=postgres dbname=dev sslmode=disable\""
	make run-bin 


