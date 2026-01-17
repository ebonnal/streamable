.PHONY: all help venv test type-check format format-check lint lint-check docs

VENV_DIR := .venv

all: venv format lint type-check test docs
qa: format lint type-check

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
	uv venv $(VENV_DIR) --clear
	uv pip install -e ".[dev]"

test:
	$(VENV_DIR)/bin/python -m pytest --cov=streamable --cov-report=term-missing --cov-fail-under=100 -sx

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
	uv pip install -e ".[docs]"
	sphinx-build -b html docs docs/_build/html
