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
- Cada push a `develop` ejecuta `.github/workflows/release.yml`
- Ese workflow construye binarios para:
  - Linux
  - macOS
  - Windows
- Los artefactos quedan publicados en la propia ejecución de GitHub Actions
- Si además el push corresponde a un tag semver `vX.Y.Z`, el mismo workflow publica un **GitHub Release** con los assets adjuntos.
- Antes del build multi‑plataforma se ejecuta un job `quality` en Ubuntu con `make lint`, `make typecheck`, `make test` y `make ci`.
- Los assets se verifican antes de subirlos con `actions/upload-artifact`.
- En Windows el venv actualiza `pip` con `python -m pip` para evitar el bloqueo de `.venv/Scripts/pip.exe`.
- El workflow usa permisos mínimos: `contents: read` por defecto y `contents: write` solo en `publish`.

Secrets opcionales para firma/notarización macOS:
- `MACOS_CODESIGN_IDENTITY`
- `APPLE_ID`
- `APPLE_APP_PASSWORD`
- `APPLE_TEAM_ID`

Si no existen, la build de macOS sigue ejecutándose y la notarización se omite.

---

## 3) Por qué 3 runners
PyInstaller **no cross-compila**: cada binario debe construirse en su OS nativo.

El runner macOS usa label Intel soportado por GitHub-hosted runners (`macos-15-intel`) porque la app se distribuye como build nativa Intel.
