.DEFAULT_GOAL := default

PYTHON ?= python3.9
VENV ?= .venv
FRONTEND_DIR ?= frontend
ROOT := $(CURDIR)
APP_PORT ?= 8000

PIP = $(VENV)/bin/pip
PY = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
BLACK = $(VENV)/bin/black
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest
NPM = npm --prefix $(FRONTEND_DIR)
PLAYWRIGHT = $(FRONTEND_DIR)/node_modules/.bin/playwright

.PHONY: default setup frontend-install frontend-build frontend-test frontend-e2e run test ci clean

default:
	@echo ""
	@echo "Comandos disponibles:"
	@printf "  %-18s %s\n" "setup" "Recrea .venv, instala backend y dependencias frontend"
	@printf "  %-18s %s\n" "run" "Construye React y levanta la API sirviendo el frontend"
	@printf "  %-18s %s\n" "test" "Ejecuta pytest backend con cobertura"
	@printf "  %-18s %s\n" "ci" "Ejecuta backend + frontend + E2E"
	@printf "  %-18s %s\n" "clean" "Limpia caches, builds y node_modules"
	@echo ""

setup:
	$(MAKE) clean
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -e ".[dev]"
	$(MAKE) frontend-install

frontend-install:
	$(NPM) install

frontend-build:
	@test -d "$(FRONTEND_DIR)/node_modules" || $(MAKE) frontend-install
	$(NPM) run build

frontend-test:
	@test -d "$(FRONTEND_DIR)/node_modules" || $(MAKE) frontend-install
	$(NPM) run test

frontend-e2e:
	@test -d "$(FRONTEND_DIR)/node_modules" || $(MAKE) frontend-install
	@test -x "$(PLAYWRIGHT)" || $(MAKE) frontend-install
	cd $(FRONTEND_DIR) && npx playwright install chromium
	$(NPM) run e2e

run:
	@test -x "$(PY)" || $(MAKE) setup
	$(MAKE) frontend-build
	@SELECTED_PORT=`APP_PORT="$(APP_PORT)" $(PY) scripts/select_port.py`; \
	if [ "$$SELECTED_PORT" != "$(APP_PORT)" ]; then \
		echo "Puerto $(APP_PORT) ocupado; usando $$SELECTED_PORT"; \
	fi; \
	NPS_LENS_FRONTEND_DIST_DIR="$(ROOT)/$(FRONTEND_DIR)/dist" $(PY) -m nps_lens.cli serve --host 127.0.0.1 --port $$SELECTED_PORT

test:
	@test -x "$(PYTEST)" || $(MAKE) setup
	$(PYTEST) --override-ini addopts="" -q --cov=src/nps_lens --cov-report=term-missing --cov-fail-under=80

ci:
	@test -x "$(PY)" || $(MAKE) setup
	$(RUFF) check --no-fix .
	$(BLACK) --check .
	$(MYPY) .
	$(MAKE) test
	$(MAKE) frontend-test
	$(MAKE) frontend-build
	$(MAKE) frontend-e2e

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist
	rm -rf $(FRONTEND_DIR)/dist $(FRONTEND_DIR)/node_modules $(FRONTEND_DIR)/playwright-report $(FRONTEND_DIR)/test-results
