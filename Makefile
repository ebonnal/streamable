VENV_DIR := .venv

all: venv test type-check format

help:
	@echo "Available commands:"
	@echo "  make all             - Run all tasks: venv, test, type-check, format"
	@echo "  make venv            - Create a virtual environment and install dependencies"
	@echo "  make test            - Run unittests and check coverage"
	@echo "  make type-check      - Check typing with mypy"
	@echo "  make format          - Format the codebase"

venv:
	python3 -m venv $(VENV_DIR) --clear
	$(VENV_DIR)/bin/pip install -r requirements-dev.txt

test:
	$(VENV_DIR)/bin/python -m coverage run -m unittest --failfast
	$(VENV_DIR)/bin/coverage report -m
	$(VENV_DIR)/bin/coverage html

type-check:
	$(VENV_DIR)/bin/python -m mypy --install-types --non-interactive streamable tests

format:
	$(VENV_DIR)/bin/python -m ruff format streamable tests

format-check:
	$(VENV_DIR)/bin/python -m ruff format --check streamable tests
