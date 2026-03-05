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

.PHONY: ci clean format help lint run setup test typecheck check-python platform

check-python: ## Fail fast if not running on Python 3.9.x (corp target)
	@$(PYTHON) -c "import sys; v=sys.version_info; ok=(v.major==3 and v.minor==9);\
	print(f'Python {v.major}.{v.minor}.{v.micro}');\
	import sys as _s; _s.exit(0 if ok else 1)" || (echo "ERROR: Python 3.9.x required (target 3.9.13)." && exit 1)

help: ## Show this help
	@awk 'BEGIN {FS=":.*##"} /^[a-zA-Z0-9_\-]+:.*##/ {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)


setup: check-python ## Create virtualenv and install all dependencies (incl. Excel engine + dev tools)
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

platform: ## Run platform batch (requires CONFIG=path/to/batch.json)
	@test -n "$(CONFIG)" || (echo "Usage: make platform CONFIG=configs/batch.json" && exit 2)
	$(VENV)/bin/nps-lens platform-batch $(CONFIG) --out-root artifacts

ci: check-python format lint typecheck test ## Run full CI pipeline locally

clean: ## Remove virtualenv and caches
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache dist build .coverage htmlcov
