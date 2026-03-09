# Release y builds (PyInstaller + GitHub Releases)

---

## 1) Builds locales
En macOS / Linux:
```bash
make setup
make build
```

Notas:
- `make build` instala automáticamente las dependencias del extra `build` antes de empaquetar.
- Ese extra incluye `PyInstaller` y `Pillow`, necesario para `scripts/prepare_icons.py`.
- La firma macOS es opcional. Si no defines `MACOS_CODESIGN_IDENTITY`, el binario se construye sin firmar.

Salida:
- `build/pyinstaller/<os>/dist/nps-lens`

---

## 2) Release automático en GitHub
- Empuja un tag semver: `vX.Y.Z`
- Se ejecuta `.github/workflows/release.yml`
- Se crean binarios para:
  - Linux
  - macOS
  - Windows
- Se publica un **GitHub Release** con assets adjuntos.

Secrets opcionales para firma/notarización macOS:
- `MACOS_CODESIGN_IDENTITY`
- `APPLE_ID`
- `APPLE_APP_PASSWORD`
- `APPLE_TEAM_ID`

Si no existen, la build de macOS sigue ejecutándose y la notarización se omite.

---

## 3) Por qué 3 runners
PyInstaller **no cross-compila**: cada binario debe construirse en su OS nativo.
