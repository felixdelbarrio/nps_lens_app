.DEFAULT_GOAL := help
PYTHON ?= python3.9
VENV ?= .venv
PIP = $(VENV)/bin/pip
PY = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
BLACK = $(VENV)/bin/black
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest
STREAMLIT = $(VENV)/bin/streamlit

.PHONY: ci clean format help lint run setup test typecheck

help: ## Show this help
	@awk 'BEGIN {FS=":.*##"} /^[a-zA-Z0-9_\-]+:.*##/ {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)


setup: ## Create virtualenv and install all dependencies (incl. Excel engine + dev tools)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -e ".[dev]"

format: ## Auto-fix lint + format code
	$(RUFF) check --fix .
	$(BLACK) .

lint: ## Run ruff lint checks
	$(RUFF) check .

typecheck: ## Run mypy type checking
	$(MYPY) .

test: ## Run pytest
	$(PYTEST)

run: ## Run Streamlit app
	$(STREAMLIT) run app/streamlit_app.py

ci: format lint typecheck test ## Run full CI pipeline locally

clean: ## Remove virtualenv and caches
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache dist build .coverage htmlcov
