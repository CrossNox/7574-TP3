SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/CrossNox/7574-TP3
DOCKER_BIN=docker
DOCKER_COMPOSE_BIN=docker-compose

export DOCKER_BUILDKIT = 1
export LAZARUS_DATADIR = ${PWD}/lazarus_data

SAMPLE_SIZE := 0.01

default: down up logs

up: docker-image
	docker-compose -f docker/docker-compose.yaml up --detach
.PHONY: up

logs:
	docker-compose -f docker/docker-compose.yaml logs -f
.PHONY: logs

down:
	@# Remove any lingering container connected to lazarus_net
	docker network inspect --format '{{range $$v := .Containers}}{{printf "%s\n" $$v.Name}}{{end}}' "lazarus_net" | xargs --no-run-if-empty docker stop
	docker-compose -f docker/docker-compose.yaml stop --timeout 1
	docker-compose -f docker/docker-compose.yaml down --remove-orphans
.PHONY: down

docker-image:
	$(DOCKER_BIN) build -f ./docker/Dockerfile -t "7574-tp3:latest" .
	$(DOCKER_BIN) build -f ./rabbitmq/Dockerfile -t "rabbitmq:latest" .
.PHONY: docker-image

download-data:
	$(DOCKER_BIN) run -v $(CURDIR)/data:/data -v $(HOME)/.kaggle:/.kaggle -e KAGGLE_JSON_LOC=/.kaggle -e DATA_OUTPUTDIR=/data --entrypoint poetry 7574-tp3:latest run lazarus dataset download --sample-size $(SAMPLE_SIZE)

pyreverse:
	poetry run pyreverse lazaurs --output=png --output-directory=informe/images/ --colorized --ignore=cli,client
	poetry run pyreverse lazaurs.dag --output=png --output-directory=informe/images/dag/ --colorized
	poetry run pyreverse lazaurs.tasks --output=png --output-directory=informe/images/tasks/ --colorized
