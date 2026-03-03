PYTHON ?= python3.9
VENV ?= .venv
PIP = $(VENV)/bin/pip
PY = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
BLACK = $(VENV)/bin/black
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest
STREAMLIT = $(VENV)/bin/streamlit

.PHONY: setup format lint typecheck test run ci clean

setup:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -e ".[dev]"

format:
	$(RUFF) check --fix .
	$(BLACK) .

lint:
	$(RUFF) check .

typecheck:
	$(MYPY) .

test:
	$(PYTEST)

run:
	$(STREAMLIT) run app/streamlit_app.py

ci: format lint typecheck test

clean:
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache dist build .coverage htmlcov
