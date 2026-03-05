# Release y builds (PyInstaller + GitHub Releases)

---

## 1) Builds locales
En macOS / Linux:
```bash
make setup
make build
```

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

---

## 3) Por qué 3 runners
PyInstaller **no cross-compila**: cada binario debe construirse en su OS nativo.

