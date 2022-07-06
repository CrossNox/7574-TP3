SHELL := /bin/bash
PWD := $(shell pwd)

default: down up logs

up: docker-image
	docker-compose -f ./tests/protocol/docker-compose-test.yaml up
.PHONY: up

servers: docker-image
	docker-compose -f ./tests/protocol/docker-compose-server.yaml up
.PHONY: up

client: docker-image
	docker-compose -f ./tests/protocol/docker-compose-client.yaml up
.PHONY: up

logs:
	docker-compose -f ./tests/protocol/docker-compose-test.yaml logs -f
.PHONY: logs

down:
	docker-compose -f ./tests/protocol/docker-compose-server.yaml stop -t 5
	docker-compose -f ./tests/protocol/docker-compose-server.yaml down --remove-orphans
	sudo rm -rf ./tests/protocol/db-data
	docker-compose -f ./tests/protocol/docker-compose-client.yaml stop -t 5
.PHONY: down

docker-image:
	docker build -f ./tests/protocol/Dockerfile -t "7574-tp3-protocol:latest" .
.PHONY: docker-image
