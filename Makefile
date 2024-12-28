VENV_DIR := .venv

all: venv test type-check lint

help:
	@echo "Available commands:"
	@echo "  make all             - Run all tasks: venv, test, type-check, lint"
	@echo "  make venv            - Create a virtual environment and install dependencies"
	@echo "  make test            - Run unittests and check coverage"
	@echo "  make type-check      - Check typing with mypy"
	@echo "  make lint            - Lint the codebase"
	@echo "  make clean           - Clean up the environment"

venv:
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install -r requirements-dev.txt

test:
	$(VENV_DIR)/bin/python -m coverage run -m unittest --failfast
	$(VENV_DIR)/bin/coverage report
	$(VENV_DIR)/bin/coverage html

type-check:
	$(VENV_DIR)/bin/python -m mypy --install-types --non-interactive streamable tests

lint:
	$(VENV_DIR)/bin/python -m autoflake --in-place --remove-all-unused-imports --remove-unused-variables --ignore-init-module -r streamable tests
	$(VENV_DIR)/bin/python -m isort streamable tests
	$(VENV_DIR)/bin/python -m black .
