.DEFAULT_GOAL := help
PYTHON ?= python3.9
VENV ?= .venv
CLI_SCRIPT ?= src/nps_lens/cli.py
DESKTOP_SCRIPT ?= src/nps_lens/desktop.py
APP_PORT ?= 8617
ICON_SOURCE ?= assets/logo.png
ICON_DIR ?= build/icons
ICON_PNG ?= $(ICON_DIR)/app.png
ICON_ICO ?= $(ICON_DIR)/app.ico
ICON_ICNS ?= $(ICON_DIR)/app.icns
MACOS_BUNDLE_ID ?= com.npslens.app
MACOS_CODESIGN_IDENTITY ?=
MACOS_ENTITLEMENTS ?= packaging/macos/entitlements.plist
PIP = $(VENV)/bin/pip
PY = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
BLACK = $(VENV)/bin/black
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest
STREAMLIT = $(VENV)/bin/streamlit

.PHONY: ci clean format format-check help lint run run-web setup ensure-venv test typecheck check-python verify-runtime platform build build-linux build-mac prepare-icons

check-python: ## Fail fast if not running on Python 3.9.x (corp target)
	@$(PYTHON) -c "import sys; v=sys.version_info; ok=(v.major==3 and v.minor==9);\
	print(f'Python {v.major}.{v.minor}.{v.micro}');\
	import sys as _s; _s.exit(0 if ok else 1)" || (echo "ERROR: Python 3.9.x required (target 3.9.13)." && exit 1)

help: ## Show this help
	@awk 'BEGIN {FS=":.*##"} /^[a-zA-Z0-9_\-]+:.*##/ {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)


setup: check-python ## Create virtualenv and install runtime + dev dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -U pip
	$(PIP) install -e ".[dev]"
	$(MAKE) verify-runtime

ensure-venv: ## Create env only when missing (faster repeated runs/builds)
	@test -x "$(PY)" || $(MAKE) setup

verify-runtime: ensure-venv ## Smoke-check critical runtime dependencies (PPT/export included)
	$(PY) scripts/verify_runtime.py

format: ## Auto-fix lint + format code
	$(RUFF) check --fix .
	$(BLACK) .

format-check: ## Check formatting only (no changes)
	$(BLACK) --check .

lint: ## Run ruff lint checks
	$(RUFF) check --no-fix .

typecheck: ## Run mypy type checking
	$(MYPY) .

test: ## Run pytest
	$(PYTEST)

run: ensure-venv ## Run desktop app container (no browser)
	NPS_LENS_PORT=$(APP_PORT) $(PY) -m nps_lens.desktop

run-web: ensure-venv ## Run Streamlit in browser (legacy/dev fallback)
	$(STREAMLIT) run app/streamlit_app.py --server.port $(APP_PORT)

platform: ## Run platform batch (requires CONFIG=path/to/batch.json)
	@test -n "$(CONFIG)" || (echo "Usage: make platform CONFIG=configs/batch.json" && exit 2)
	$(VENV)/bin/nps-lens platform-batch $(CONFIG) --out-root artifacts

build: ensure-venv ## Build native executable with PyInstaller (macOS/Linux). Windows builds run in GitHub Actions.
	$(MAKE) verify-runtime
	@uname_s=$$(uname -s); \
	if [ "$$uname_s" = "Darwin" ]; then \
		$(MAKE) build-mac; \
	elif [ "$$uname_s" = "Linux" ]; then \
		$(MAKE) build-linux; \
	else \
		echo "Unsupported OS for local build: $$uname_s"; \
		echo "Use GitHub Actions workflow build-windows.yml for Windows builds."; \
		exit 1; \
	fi

build-linux: ensure-venv ## Build Linux binary with PyInstaller (+ Pillow icon tooling)
	$(PIP) install -e ".[build]"
	$(MAKE) prepare-icons
	@out=build/pyinstaller/linux; \
	mkdir -p $$out/dist $$out/work $$out/spec; \
	$(VENV)/bin/pyinstaller --clean --noconfirm \
		--name nps-lens \
		--onefile \
		--icon "$(PWD)/$(ICON_PNG)" \
		--add-data="$(PWD)/app:app" \
		--add-data="$(PWD)/assets:assets" \
		--add-data="$(PWD)/.streamlit:.streamlit" \
		--collect-submodules webview \
		--collect-all kaleido \
		--collect-data pptx \
		--distpath $$out/dist \
		--workpath $$out/work \
		--specpath $$out/spec \
		$(DESKTOP_SCRIPT)
	@echo "Built: build/pyinstaller/linux/dist/nps-lens"

build-mac: ensure-venv ## Build macOS binary with optional signing/notarization inputs
	$(PIP) install -e ".[build]"
	$(MAKE) prepare-icons
	@out=build/pyinstaller/macos; \
	mkdir -p $$out/dist $$out/work $$out/spec; \
	set -- \
		--clean \
		--noconfirm \
		--name nps-lens \
		--onefile \
		--icon "$(PWD)/$(ICON_ICNS)" \
		--add-data="$(PWD)/app:app" \
		--add-data="$(PWD)/assets:assets" \
		--add-data="$(PWD)/.streamlit:.streamlit" \
		--collect-submodules webview \
		--collect-all kaleido \
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
	$(VENV)/bin/pyinstaller "$$@"
	@echo "Built: build/pyinstaller/macos/dist/nps-lens"

prepare-icons: ensure-venv ## Generate .png/.ico/.icns from assets/logo.png (requires Pillow via .[build])
	$(PY) scripts/prepare_icons.py --input $(ICON_SOURCE) --out-dir $(ICON_DIR)

ci: check-python ensure-venv lint format-check typecheck test ## Run full CI checks locally

clean: ## Remove virtualenv and caches
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache dist build .coverage htmlcov
