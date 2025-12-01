.PHONY: all help venv test type-check format format-check lint lint-check docs

VENV_DIR := .venv

all: venv test type-check format lint docs

help:
	@echo "Available commands:"
	@echo "  make all             - Run all tasks: venv, test, type-check, format"
	@echo "  make venv            - Create a virtual environment and install dependencies"
	@echo "  make test            - Run unit tests and check coverage"
	@echo "  make type-check      - Check typing via mypy"
	@echo "  make format          - Format via ruff"
	@echo "  make format-check    - Check the formatting via ruff"
	@echo "  make docs            - Build the docs via sphinx

venv:
	python3 -m venv $(VENV_DIR) --clear
	$(VENV_DIR)/bin/pip install -r requirements-dev.txt
	$(VENV_DIR)/bin/pip install -U pip setuptools wheel
	$(VENV_DIR)/bin/pip install -e .

test:
	$(VENV_DIR)/bin/python -m coverage run -m pytest -sx
	$(VENV_DIR)/bin/coverage report -m
	$(VENV_DIR)/bin/coverage html

type-check:
	$(VENV_DIR)/bin/python -m mypy --install-types --non-interactive streamable tests

format:
	$(VENV_DIR)/bin/python -m ruff format streamable tests

format-check:
	$(VENV_DIR)/bin/python -m ruff format --check streamable tests

lint:
	$(VENV_DIR)/bin/python -m ruff check streamable tests --fix

lint-check:
	$(VENV_DIR)/bin/python -m ruff check streamable tests

docs:
	$(VENV_DIR)/bin/pip install -r docs/requirements.txt
	sphinx-build -b html docs docs/_build/html
