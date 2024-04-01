.PHONY: help install-dev

export PYTHONPATH := $(shell pwd)

help:
	@echo "make help - display this message"
	@echo "make install-dev - install development dependencies"

install-dev:
	uv pip install -r requirements/dev.txt -r requirements/test.txt