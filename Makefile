.DEFAULT_GOAL := default

VENV ?= .venv

ifeq ($(OS),Windows_NT)
PYTHON ?= python
VENV_BIN = $(VENV)/Scripts
BIN_EXT := .exe
PLAYWRIGHT := $(FRONTEND_DIR)/node_modules/.bin/playwright.cmd
else
PYTHON ?= python3.9
VENV_BIN = $(VENV)/bin
BIN_EXT :=
PLAYWRIGHT = $(FRONTEND_DIR)/node_modules/.bin/playwright
endif

FRONTEND_DIR ?= frontend
DESKTOP_SCRIPT ?= src/nps_lens/desktop.py
ICON_SOURCE ?= assets/logo.png
ICON_DIR ?= build/icons
ICON_PNG ?= $(ICON_DIR)/app.png
ICON_ICO ?= $(ICON_DIR)/app.ico
ICON_ICNS ?= $(ICON_DIR)/app.icns
ifeq ($(OS),Windows_NT)
ICON_RUNTIME ?= $(ICON_ICO)
else
UNAME_S := $(shell uname -s 2>/dev/null)
ifeq ($(UNAME_S),Darwin)
ICON_RUNTIME ?= $(ICON_ICNS)
else
ICON_RUNTIME ?= $(ICON_PNG)
endif
endif
MACOS_BUNDLE_ID ?= com.npslens.app
MACOS_CODESIGN_IDENTITY ?=
MACOS_ENTITLEMENTS ?= packaging/macos/entitlements.plist
MACOS_INSTALL_TO_APPLICATIONS ?= 0
ROOT := $(CURDIR)
APP_PORT ?= 8617

PIP = $(VENV_BIN)/pip$(BIN_EXT)
PY = $(VENV_BIN)/python$(BIN_EXT)
RUFF = $(VENV_BIN)/ruff$(BIN_EXT)
BLACK = $(VENV_BIN)/black$(BIN_EXT)
MYPY = $(VENV_BIN)/mypy$(BIN_EXT)
PYTEST = $(VENV_BIN)/pytest$(BIN_EXT)
NPM = npm --prefix $(FRONTEND_DIR)

.PHONY: default venv python-dev python-build setup frontend-install frontend-build frontend-test frontend-e2e build run lint typecheck test ci clean

default:
	@echo ""
	@echo "Comandos disponibles:"
	@printf "  %-18s %s\n" "setup" "Recrea .venv e instala dependencias backend/frontend/build"
	@printf "  %-18s %s\n" "build" "Compila el frontend y empaqueta la app de escritorio"
	@printf "  %-18s %s\n" "run" "Construye React y arranca la app de escritorio nativa"
	@printf "  %-18s %s\n" "lint" "Ejecuta ruff y black en modo verificación"
	@printf "  %-18s %s\n" "typecheck" "Ejecuta mypy sobre el código backend tipado"
	@printf "  %-18s %s\n" "test" "Ejecuta pytest backend con cobertura"
	@printf "  %-18s %s\n" "ci" "Ejecuta lint backend + frontend + E2E"
	@printf "  %-18s %s\n" "clean" "Limpia caches, builds y node_modules"
	@echo ""

venv:
	@test -x "$(PY)" || $(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip

python-dev:
	$(MAKE) venv
	$(PIP) install -e ".[dev]"

python-build:
	$(MAKE) venv
	$(PIP) install -e ".[build]"

setup:
	$(MAKE) clean
	rm -rf $(VENV)
	$(MAKE) venv
	$(PIP) install -e ".[dev,build]"
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

build:
	$(MAKE) python-build
	$(MAKE) frontend-build
	find build/pyinstaller -name '.DS_Store' -delete 2>/dev/null || true
	rm -rf build/pyinstaller dist || true
	rm -rf $(ICON_DIR)
	$(PY) scripts/prepare_icons.py --input $(ICON_SOURCE) --out-dir $(ICON_DIR)
	@uname_s=$$(uname -s); \
	if [ "$$uname_s" = "Darwin" ]; then \
		out=build/pyinstaller/macos; \
		mkdir -p $$out/dist $$out/work $$out/spec; \
		set -- \
			--clean \
			--noconfirm \
			--name nps-lens \
			--windowed \
			--icon "$(ROOT)/$(ICON_ICNS)" \
			--add-data="$(ROOT)/frontend/dist:frontend/dist" \
			--add-data="$(ROOT)/assets:assets" \
			--add-data="$(ROOT)/$(ICON_DIR):build/icons" \
			--add-data="$(ROOT)/.env.example:." \
			--collect-submodules nps_lens \
			--collect-submodules webview \
			--copy-metadata python-dotenv \
			--copy-metadata pywebview \
			--copy-metadata fastapi \
			--copy-metadata uvicorn \
			--collect-data pptx \
			--distpath $$out/dist \
			--workpath $$out/work \
			--specpath $$out/spec \
			--osx-bundle-identifier "$(MACOS_BUNDLE_ID)" \
			"$(DESKTOP_SCRIPT)"; \
		if [ -n "$(MACOS_CODESIGN_IDENTITY)" ]; then \
			set -- "$$@" --codesign-identity "$(MACOS_CODESIGN_IDENTITY)"; \
			if [ -f "$(MACOS_ENTITLEMENTS)" ]; then \
				set -- "$$@" --osx-entitlements-file "$(MACOS_ENTITLEMENTS)"; \
			fi; \
			echo "macOS signing enabled for identity: $(MACOS_CODESIGN_IDENTITY)"; \
		else \
			echo "macOS signing disabled (set MACOS_CODESIGN_IDENTITY to enable)."; \
		fi; \
		"$(VENV_BIN)/pyinstaller$(BIN_EXT)" "$$@"; \
		echo "Built app: $$out/dist/nps-lens.app"; \
		if [ "$(MACOS_INSTALL_TO_APPLICATIONS)" = "1" ]; then \
			app_dst="/Applications/nps-lens.app"; \
			rm -rf "$$app_dst" 2>/dev/null || true; \
			if cp -R "$$out/dist/nps-lens.app" "$$app_dst" 2>/dev/null; then \
				echo "Installed app: $$app_dst"; \
			else \
				echo "Could not copy app to /Applications."; \
			fi; \
		fi; \
	elif [ "$$uname_s" = "Linux" ]; then \
		out=build/pyinstaller/linux; \
		mkdir -p $$out/dist $$out/work $$out/spec; \
		"$(VENV_BIN)/pyinstaller$(BIN_EXT)" --clean --noconfirm \
			--name nps-lens \
			--onefile \
			--icon "$(ROOT)/$(ICON_PNG)" \
			--add-data="$(ROOT)/frontend/dist:frontend/dist" \
			--add-data="$(ROOT)/assets:assets" \
			--add-data="$(ROOT)/$(ICON_DIR):build/icons" \
			--add-data="$(ROOT)/.env.example:." \
			--collect-submodules nps_lens \
			--collect-submodules webview \
			--copy-metadata python-dotenv \
			--copy-metadata pywebview \
			--copy-metadata fastapi \
			--copy-metadata uvicorn \
			--collect-data pptx \
			--distpath $$out/dist \
			--workpath $$out/work \
			--specpath $$out/spec \
			"$(DESKTOP_SCRIPT)"; \
		echo "Built binary: $$out/dist/nps-lens"; \
	elif [ "$(OS)" = "Windows_NT" ] || printf "%s" "$$uname_s" | grep -Eq 'MINGW|MSYS|CYGWIN'; then \
		out=build/pyinstaller/windows; \
		mkdir -p $$out/dist $$out/work $$out/spec; \
		"$(VENV_BIN)/pyinstaller$(BIN_EXT)" --clean --noconfirm \
			--name nps-lens \
			--windowed \
			--icon "$(ROOT)/$(ICON_ICO)" \
			--add-data="$(ROOT)/frontend/dist;frontend/dist" \
			--add-data="$(ROOT)/assets;assets" \
			--add-data="$(ROOT)/$(ICON_DIR);build/icons" \
			--add-data="$(ROOT)/.env.example;." \
			--collect-submodules nps_lens \
			--collect-submodules webview \
			--copy-metadata python-dotenv \
			--copy-metadata pywebview \
			--copy-metadata fastapi \
			--copy-metadata uvicorn \
			--collect-data pptx \
			--distpath $$out/dist \
			--workpath $$out/work \
			--specpath $$out/spec \
			"$(DESKTOP_SCRIPT)"; \
		echo "Built binary: $$out/dist/nps-lens.exe"; \
	else \
		echo "Unsupported OS for local build: $$uname_s"; \
		exit 1; \
	fi

run:
	$(MAKE) python-build
	$(MAKE) frontend-build
	rm -rf $(ICON_DIR)
	$(PY) scripts/prepare_icons.py --input $(ICON_SOURCE) --out-dir $(ICON_DIR)
	NPS_LENS_PORT="$(APP_PORT)" \
	NPS_LENS_ICON="$(ROOT)/$(ICON_RUNTIME)" \
	NPS_LENS_FRONTEND_DIST_DIR="$(ROOT)/$(FRONTEND_DIR)/dist" \
	$(PY) -m nps_lens.desktop

lint:
	@test -x "$(RUFF)" && test -x "$(BLACK)" || $(MAKE) python-dev
	$(RUFF) check --no-fix .
	$(BLACK) --check .

typecheck:
	@test -x "$(MYPY)" || $(MAKE) python-dev
	$(MYPY) .

test:
	@test -x "$(PYTEST)" || $(MAKE) python-dev
	$(PYTEST) --override-ini addopts="" -q --cov=src/nps_lens --cov-report=term-missing --cov-fail-under=80

ci:
	@test -x "$(PY)" && test -x "$(RUFF)" && test -x "$(BLACK)" || $(MAKE) python-dev
	$(MAKE) lint
	$(MAKE) frontend-test
	$(MAKE) frontend-build
	$(MAKE) frontend-e2e

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist
	rm -rf $(FRONTEND_DIR)/dist $(FRONTEND_DIR)/node_modules $(FRONTEND_DIR)/playwright-report $(FRONTEND_DIR)/test-results $(FRONTEND_DIR)/.playwright-data
