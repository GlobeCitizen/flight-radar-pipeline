.PHONY: help install-dev

export PYTHONPATH := $(shell pwd)

question ?= --help
scale ?= 1

help:
	@echo "make install-dev - install development dependencies"
	@echo "make build - build the Docker Compose services"
	@echo "make up - start the Docker Compose services"
	@echo "make down - stop the Docker Compose services"
	@echo "make run-main - run the main.py script"
	@echo "make run-answers - run the answers.py script"

install-dev:
	uv pip install -r requirements/dev.txt -r requirements/test.txt

build:
	docker-compose build

up:
	docker-compose up -d --scale spark-worker=$(scale)

down:
	docker-compose down

run-main:
	docker-compose exec my-app python main.py

run-answers:
	docker-compose exec my-app python answers.py $(question)