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
WEBAPP_PORT ?= 8625

# Frontend Playwright browser cache (E2E).
PLAYWRIGHT_BROWSERS_PATH ?= $(ROOT)/$(FRONTEND_DIR)/.playwright-browsers

# Python Playwright browsers MUST live outside site-packages.
# Keeping them in PLAYWRIGHT_BROWSERS_PATH=0 makes the Playwright PyInstaller
# hook classify the embedded Chrome.app binaries individually on macOS, which
# breaks codesigning during COLLECT. We package this directory ourselves after
# PyInstaller has finished, preserving Chrome's own bundle/signature structure.
PY_PLAYWRIGHT_BROWSERS_PATH ?= $(VENV)/.playwright-browsers
PYINSTALLER_RUNTIME_HOOK ?= build/pyinstaller/runtime_hook_playwright.py

PIP = $(VENV_BIN)/pip$(BIN_EXT)
PY = $(VENV_BIN)/python$(BIN_EXT)
RUFF = $(VENV_BIN)/ruff$(BIN_EXT)
BLACK = $(VENV_BIN)/black$(BIN_EXT)
MYPY = $(VENV_BIN)/mypy$(BIN_EXT)
PYTEST = $(VENV_BIN)/pytest$(BIN_EXT)
NPM = npm --prefix $(FRONTEND_DIR)

.PHONY: default venv python-dev python-build python-playwright setup frontend-install frontend-build frontend-test frontend-e2e build run kill webapp WebApp lint typecheck test ci clean

default:
	@echo ""
	@echo "Comandos disponibles:"
	@printf "  %-18s %s\n" "setup" "Recrea .venv e instala dependencias backend/frontend/build"
	@printf "  %-18s %s\n" "build" "Compila el frontend y empaqueta la app de escritorio"
	@printf "  %-18s %s\n" "run" "Construye React y arranca la app de escritorio nativa"
	@printf "  %-18s %s\n" "kill" "Detiene solo las instancias locales de NPS Lens"
	@printf "  %-18s %s\n" "webapp / WebApp" "Prueba la WebApp estática con la última edición generada"
	@printf "  %-18s %s\n" "lint" "Ejecuta ruff y black en modo verificación"
	@printf "  %-18s %s\n" "typecheck" "Ejecuta mypy sobre el código backend tipado"
	@printf "  %-18s %s\n" "test" "Ejecuta pytest backend con cobertura"
	@printf "  %-18s %s\n" "ci" "Ejecuta lint backend + frontend + E2E"
	@printf "  %-18s %s\n" "clean" "Limpia caches, builds y node_modules"
	@echo ""

venv:
	@test -x "$(PY)" || $(PYTHON) -m venv $(VENV)
	$(PY) -m pip install -U pip

python-dev:
	$(MAKE) venv
	$(PIP) install -e ".[dev]"
	$(MAKE) python-playwright

python-build:
	$(MAKE) venv
	$(PIP) install -e ".[build]"
	$(MAKE) python-playwright

python-playwright:
	@legacy_browser_dir="$$($(PY) -c 'from pathlib import Path; import playwright; print(Path(playwright.__file__).resolve().parent / "driver" / "package" / ".local-browsers")')"; \
	if [ -d "$$legacy_browser_dir" ]; then \
		echo "Removing legacy Playwright browsers from site-packages: $$legacy_browser_dir"; \
		rm -rf "$$legacy_browser_dir"; \
	fi
	@mkdir -p "$(PY_PLAYWRIGHT_BROWSERS_PATH)"
	PLAYWRIGHT_BROWSERS_PATH="$(abspath $(PY_PLAYWRIGHT_BROWSERS_PATH))" $(PY) -m playwright install chromium

setup:
	$(MAKE) clean
	rm -rf $(VENV)
	$(MAKE) venv
	$(PIP) install -e ".[dev,build]"
	$(MAKE) python-playwright
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
	cd $(FRONTEND_DIR) && PLAYWRIGHT_BROWSERS_PATH="$(PLAYWRIGHT_BROWSERS_PATH)" npx playwright install chromium
	PLAYWRIGHT_BROWSERS_PATH="$(PLAYWRIGHT_BROWSERS_PATH)" $(NPM) run e2e

build:
	$(MAKE) python-build
	$(MAKE) frontend-build
	find build/pyinstaller -name '.DS_Store' -delete 2>/dev/null || true
	rm -rf build/pyinstaller dist || true
	rm -rf $(ICON_DIR)
	$(PY) scripts/prepare_icons.py --input $(ICON_SOURCE) --out-dir $(ICON_DIR)
	@mkdir -p "$$(dirname "$(PYINSTALLER_RUNTIME_HOOK)")"
	@printf '%s\n' \
		'import os' \
		'import sys' \
		'from pathlib import Path' \
		'' \
		'if getattr(sys, "frozen", False):' \
		'    executable = Path(sys.executable).resolve()' \
		'    if sys.platform == "darwin":' \
		'        browser_dir = executable.parent.parent / "Resources" / "playwright-browsers"' \
		'    else:' \
		'        browser_dir = executable.parent / "playwright-browsers"' \
		'    if browser_dir.is_dir():' \
		'        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browser_dir)' \
		> "$(PYINSTALLER_RUNTIME_HOOK)"
	@set -eu; \
	uname_s=$$(uname -s 2>/dev/null || printf unknown); \
	browser_src="$(abspath $(PY_PLAYWRIGHT_BROWSERS_PATH))"; \
	runtime_hook="$(abspath $(PYINSTALLER_RUNTIME_HOOK))"; \
	test -d "$$browser_src" || { echo "ERROR: Python Playwright browsers not found at $$browser_src"; exit 1; }; \
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
			--add-data="$(ROOT)/frontend/public/assets/brand:assets/brand" \
			--add-data="$(ROOT)/assets:assets" \
			--add-data="$(ROOT)/$(ICON_DIR):build/icons" \
			--add-data="$(ROOT)/.env.example:." \
			--runtime-hook "$$runtime_hook" \
			--collect-submodules nps_lens \
			--collect-submodules webview \
			--collect-all playwright \
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
			echo "macOS PyInstaller signing enabled for identity: $(MACOS_CODESIGN_IDENTITY)"; \
		else \
			echo "macOS PyInstaller uses ad-hoc signing; set MACOS_CODESIGN_IDENTITY for Developer ID signing."; \
		fi; \
		"$(VENV_BIN)/pyinstaller$(BIN_EXT)" "$$@"; \
		app="$$out/dist/nps-lens.app"; \
		test -d "$$app" || { echo "ERROR: PyInstaller did not create $$app"; exit 1; }; \
		browser_dst="$$app/Contents/Resources/playwright-browsers"; \
		rm -rf "$$browser_dst"; \
		mkdir -p "$$(dirname "$$browser_dst")"; \
		echo "Embedding Playwright browsers after PyInstaller: $$browser_dst"; \
		/usr/bin/ditto "$$browser_src" "$$browser_dst"; \
		echo "Signing embedded Playwright browser binaries before sealing the app."; \
		find "$$browser_dst" -type f -exec sh -eu -c ' \
			identity="$$1"; shift; \
			for binary do \
				if /usr/bin/file -b "$$binary" | grep -q "Mach-O"; then \
					if [ -n "$$identity" ]; then \
						/usr/bin/codesign --force --options runtime --timestamp --sign "$$identity" "$$binary"; \
					else \
						/usr/bin/codesign --force --sign - --timestamp=none "$$binary"; \
					fi; \
				fi; \
			done \
		' sh "$(MACOS_CODESIGN_IDENTITY)" {} +; \
		find "$$browser_dst" -depth -type d -name '*.app' -exec sh -eu -c ' \
			identity="$$1"; shift; \
			for browser_app do \
				if [ -n "$$identity" ]; then \
					/usr/bin/codesign --force --deep --options runtime --timestamp --sign "$$identity" "$$browser_app"; \
				else \
					/usr/bin/codesign --force --deep --sign - --timestamp=none "$$browser_app"; \
				fi; \
			done \
		' sh "$(MACOS_CODESIGN_IDENTITY)" {} +; \
		if [ -n "$(MACOS_CODESIGN_IDENTITY)" ]; then \
			echo "Sealing final app with identity: $(MACOS_CODESIGN_IDENTITY)"; \
			/usr/bin/codesign --force --options runtime --timestamp --sign "$(MACOS_CODESIGN_IDENTITY)" "$$app"; \
		else \
			echo "Sealing final app with ad-hoc signature."; \
			/usr/bin/codesign --force --sign - --timestamp=none "$$app"; \
		fi; \
		find "$$browser_dst" -depth -type d -name '*.app' \
			-exec /usr/bin/codesign --verify --deep --strict {} \;; \
		find "$$browser_dst" -type f -exec sh -eu -c ' \
			for binary do \
				if /usr/bin/file -b "$$binary" | grep -q "Mach-O"; then \
					/usr/bin/codesign --verify --strict "$$binary"; \
				fi; \
			done \
		' sh {} +; \
		/usr/bin/codesign --verify --deep --strict "$$app"; \
		echo "Built app: $$app"; \
		if [ "$(MACOS_INSTALL_TO_APPLICATIONS)" = "1" ]; then \
			app_dst="/Applications/nps-lens.app"; \
			rm -rf "$$app_dst" 2>/dev/null || true; \
			if /usr/bin/ditto "$$app" "$$app_dst" 2>/dev/null; then \
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
			--add-data="$(ROOT)/frontend/public/assets/brand:assets/brand" \
			--add-data="$(ROOT)/assets:assets" \
			--add-data="$(ROOT)/$(ICON_DIR):build/icons" \
			--add-data="$(ROOT)/.env.example:." \
			--runtime-hook "$$runtime_hook" \
			--collect-submodules nps_lens \
			--collect-submodules webview \
			--collect-all playwright \
			--copy-metadata python-dotenv \
			--copy-metadata pywebview \
			--copy-metadata fastapi \
			--copy-metadata uvicorn \
			--collect-data pptx \
			--distpath $$out/dist \
			--workpath $$out/work \
			--specpath $$out/spec \
			"$(DESKTOP_SCRIPT)"; \
		rm -rf "$$out/dist/playwright-browsers"; \
		cp -R "$$browser_src" "$$out/dist/playwright-browsers"; \
		echo "Built binary: $$out/dist/nps-lens"; \
		echo "Bundled browsers: $$out/dist/playwright-browsers"; \
	elif [ "$(OS)" = "Windows_NT" ] || printf "%s" "$$uname_s" | grep -Eq 'MINGW|MSYS|CYGWIN'; then \
		out=build/pyinstaller/windows; \
		mkdir -p $$out/dist $$out/work $$out/spec; \
		"$(VENV_BIN)/pyinstaller$(BIN_EXT)" --clean --noconfirm \
			--name nps-lens \
			--onefile \
			--windowed \
			--icon "$(ROOT)/$(ICON_ICO)" \
			--add-data="$(ROOT)/frontend/dist;frontend/dist" \
			--add-data="$(ROOT)/frontend/public/assets/brand;assets/brand" \
			--add-data="$(ROOT)/assets;assets" \
			--add-data="$(ROOT)/$(ICON_DIR);build/icons" \
			--add-data="$(ROOT)/.env.example;." \
			--runtime-hook "$$runtime_hook" \
			--collect-submodules nps_lens \
			--collect-submodules webview \
			--collect-all playwright \
			--copy-metadata python-dotenv \
			--copy-metadata pywebview \
			--copy-metadata fastapi \
			--copy-metadata uvicorn \
			--collect-data pptx \
			--distpath $$out/dist \
			--workpath $$out/work \
			--specpath $$out/spec \
			"$(DESKTOP_SCRIPT)"; \
		rm -rf "$$out/dist/playwright-browsers"; \
		cp -R "$$browser_src" "$$out/dist/playwright-browsers"; \
		echo "Built binary: $$out/dist/nps-lens.exe"; \
		echo "Bundled browsers: $$out/dist/playwright-browsers"; \
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
	PLAYWRIGHT_BROWSERS_PATH="$(abspath $(PY_PLAYWRIGHT_BROWSERS_PATH))" \
	$(PY) -m nps_lens.desktop

kill:
	@$(PYTHON) scripts/local_processes.py --root "$(ROOT)" --ports "$(APP_PORT)" 5173 "$(WEBAPP_PORT)"

webapp: kill
	@test -x "$(PY)" || $(MAKE) venv
	@$(PY) scripts/serve_webapp.py --port "$(WEBAPP_PORT)"

WebApp: webapp

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
	$(RUFF) check --no-fix .
	$(BLACK) --check .
	$(MAKE) frontend-test
	$(MAKE) frontend-build
	$(MAKE) frontend-e2e

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist
	rm -rf $(FRONTEND_DIR)/dist $(FRONTEND_DIR)/node_modules $(FRONTEND_DIR)/playwright-report $(FRONTEND_DIR)/test-results $(FRONTEND_DIR)/.playwright-data $(FRONTEND_DIR)/.playwright-browsers
