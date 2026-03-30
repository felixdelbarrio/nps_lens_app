.DEFAULT_GOAL := default

PYTHON ?= python3.9
VENV ?= .venv
DESKTOP_SCRIPT ?= src/nps_lens/desktop.py
ICON_SOURCE ?= assets/logo.png
ICON_DIR ?= build/icons
ICON_PNG ?= $(ICON_DIR)/app.png
ICON_ICO ?= $(ICON_DIR)/app.ico
ICON_ICNS ?= $(ICON_DIR)/app.icns
MACOS_BUNDLE_ID ?= com.npslens.app
MACOS_CODESIGN_IDENTITY ?=
MACOS_ENTITLEMENTS ?= packaging/macos/entitlements.plist
ROOT := $(CURDIR)
APP_PORT ?= 8617

PIP = $(VENV)/bin/pip
PY = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
BLACK = $(VENV)/bin/black
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest

.PHONY: default setup clean build run ci test

default:
	@echo ""
	@echo "Comandos disponibles:"
	@printf "  %-10s %s\n" "setup" "Recrea .venv e instala dependencias (dev+build)"
	@printf "  %-10s %s\n" "build" "Limpia builds previas, detecta OS y compila ejecutable nuevo"
	@printf "  %-10s %s\n" "run"   "Ejecuta la ultima version de codigo (sin build)"
	@printf "  %-10s %s\n" "test"  "Ejecuta pytest con coverage (umbral >= 80%)"
	@printf "  %-10s %s\n" "ci"    "Ejecuta ruff + black --check + mypy + test"
	@printf "  %-10s %s\n" "clean" "Limpia caches y artefactos de build"
	@echo ""
	@echo "Uso: make <comando>"

setup:
	$(MAKE) clean
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -e ".[dev,build]"
	$(MAKE) clean

build:
	@test -x "$(PY)" || $(MAKE) setup
	$(PIP) install -e ".[build]"
	rm -rf build/pyinstaller dist
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
			--add-data="$(ROOT)/app:app" \
			--add-data="$(ROOT)/assets:assets" \
			--add-data="$(ROOT)/.streamlit:.streamlit" \
			--collect-submodules webview \
			--collect-all kaleido \
			--collect-all streamlit \
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
		$(VENV)/bin/pyinstaller "$$@"; \
		echo "Built app: $$out/dist/nps-lens.app"; \
		echo "Built folder: $$out/dist/nps-lens"; \
	elif [ "$$uname_s" = "Linux" ]; then \
		out=build/pyinstaller/linux; \
		mkdir -p $$out/dist $$out/work $$out/spec; \
		$(VENV)/bin/pyinstaller --clean --noconfirm \
			--name nps-lens \
			--onefile \
			--icon "$(ROOT)/$(ICON_PNG)" \
			--add-data="$(ROOT)/app:app" \
			--add-data="$(ROOT)/assets:assets" \
			--add-data="$(ROOT)/.streamlit:.streamlit" \
			--collect-submodules webview \
			--collect-all kaleido \
			--collect-all streamlit \
			--collect-data pptx \
			--distpath $$out/dist \
			--workpath $$out/work \
			--specpath $$out/spec \
			"$(DESKTOP_SCRIPT)"; \
		echo "Built: $$out/dist/nps-lens"; \
	else \
		echo "Unsupported OS for local build: $$uname_s"; \
		exit 1; \
	fi

run:
	@test -x "$(PY)" || $(MAKE) setup
	$(PIP) install -e ".[build]"
	$(PY) scripts/prepare_icons.py --input $(ICON_SOURCE) --out-dir $(ICON_DIR)
	NPS_LENS_PORT=$(APP_PORT) NPS_LENS_ICON="$(ROOT)/$(ICON_PNG)" $(PY) -m nps_lens.desktop

test:
	@test -x "$(PYTEST)" || $(MAKE) setup
	$(PYTEST) --override-ini addopts="" -q --cov=src/nps_lens --cov-report=term-missing --cov-fail-under=80

ci:
	@test -x "$(PY)" || $(MAKE) setup
	$(RUFF) check --no-fix .
	$(BLACK) --check .
	$(MYPY) .
	$(MAKE) test

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache dist build .coverage htmlcov
