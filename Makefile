# Phony targets are not real files
.PHONY: all help venv test type-check format format-check

VENV_DIR := .venv

# In CI, use the system python. Otherwise, use the venv's python.
ifeq ($(CI),true)
PYTHON := python
else
PYTHON := $(VENV_DIR)/bin/python
endif

all: venv test type-check format

help:
	@echo "Available commands:"
	@echo "  make all             - Run all tasks: venv, test, type-check, format"
	@echo "  make venv            - Create a virtual environment and install dependencies"
	@echo "  make test            - Run unittests and check coverage"
	@echo "  make type-check      - Check typing via mypy"
	@echo "  make format          - Format via ruff"
	@echo "  make format-check    - Check the formatting via ruff"

venv:
	python3 -m venv $(VENV_DIR) --clear
	$(VENV_DIR)/bin/pip install -r requirements-dev.txt

test:
	$(PYTHON) -m coverage run -m unittest -v --failfast
	$(PYTHON) -m coverage report -m
	$(PYTHON) -m coverage html

type-check:
	$(PYTHON) -m mypy --install-types --non-interactive streamable tests

format:
	$(PYTHON) -m ruff format streamable tests

format-check:
	$(PYTHON) -m ruff format --check streamable tests
