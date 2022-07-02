SHELL := /bin/bash
PWD := $(shell pwd)

default: run

run:
	docker-compose -f docker-compose-test.yaml up --build
.PHONY: run

stop:
	docker-compose -f docker-compose-test.yaml stop -t 10
	docker-compose -f docker-compose-test.yaml down
.PHONY: run
