build-docker:  ## Build a docker container 
	docker build . -t ghcr.io/risingwavelabs/demo-datagen:latest

build-bin: ## Build the executable
	go build

# start kafka 
# If not already running, then start kafa

run-bin: build-bin ## Run the executable
	./datagen --mode nested --qps 1000 kafka --brokers localhost:9092


# TODO: Add help target