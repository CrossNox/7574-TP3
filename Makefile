SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/CrossNox/7574-TP3
DOCKER_BIN=docker
DOCKER_COMPOSE_BIN=docker-compose

SAMPLE_SIZE := 0.01

default: docker-image

docker-image:
	$(DOCKER_BIN) build -f ./docker/Dockerfile -t "7574-tp3:latest" .
.PHONY: docker-image

download-data:
	$(DOCKER_BIN) run -v $(CURDIR)/data:/data -v $(HOME)/.kaggle:/.kaggle -e KAGGLE_JSON_LOC=/.kaggle -e DATA_OUTPUTDIR=/data --entrypoint poetry 7574-tp3:latest run lazarus datset -vv --sample-size $(SAMPLE_SIZE)

pyreverse:
	poetry run pyreverse lazaurs --output=png --output-directory=informe/images/ --colorized --ignore=cli,client
	poetry run pyreverse lazaurs.dag --output=png --output-directory=informe/images/dag/ --colorized
	poetry run pyreverse lazaurs.tasks --output=png --output-directory=informe/images/tasks/ --colorized
