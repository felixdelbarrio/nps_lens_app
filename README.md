# NPS Lens — Plataforma de Insights VoC (NPS + Texto + Incidencias)

[![CI](https://github.com/<ORG>/<REPO>/actions/workflows/ci.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/ci.yml)
[![Type Check](https://github.com/<ORG>/<REPO>/actions/workflows/type-check.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/type-check.yml)
[![Test](https://github.com/<ORG>/<REPO>/actions/workflows/test.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/test.yml)
[![Build Linux](https://github.com/<ORG>/<REPO>/actions/workflows/build-linux.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/build-linux.yml)
[![Build macOS](https://github.com/<ORG>/<REPO>/actions/workflows/build-mac.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/build-mac.yml)
[![Build Windows](https://github.com/<ORG>/<REPO>/actions/workflows/build-windows.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/build-windows.yml)
[![Release](https://github.com/<ORG>/<REPO>/actions/workflows/release.yml/badge.svg)](https://github.com/<ORG>/<REPO>/actions/workflows/release.yml)

[![Sponsor](https://img.shields.io/badge/Sponsor-GitHub%20Sponsors-2ea44f.svg)](https://github.com/sponsors/felixdelbarrio)
[![Donate](https://img.shields.io/badge/Donate-PayPal-blue.svg)](https://paypal.me/felixdelbarrio)

> ⚠️ **Antes de publicar**: sustituye `<ORG>/<REPO>` en los badges por el `owner/repo` real.  
> Ejemplo: `bbva/nps-lens` (o el que corresponda).

---

## Qué es NPS Lens

**NPS Lens** es una plataforma para convertir señales de Voz del Cliente en **insights accionables**, combinando:

- **NPS térmico** (score + texto + palanca/subpalanca/canal/segmento)
- **Incidencias Helix** (tickets/bugs) para correlación y causalidad pragmática
- (Opcional) **Reviews** (stores) / **Feedback in‑app** (roadmap)

La aplicación une métricas + verbatims + evidencias multi‑fuente y genera un **LLM Deep‑Dive Pack** “copy/paste ready” para investigación asistida por LLM, con trazabilidad y versión de pipeline.

---

## Para qué sirve (valor de negocio)

- Detectar **drivers reales** de detracción (y también de promoción) por palanca/subpalanca/canal.
- Priorizar **causas raíz plausibles** con *causalidad pragmática* (no solo correlación).
- Construir **journeys de caída** (ruta: palanca → subpalanca → tópico → incidencia → impacto NPS).
- Operar como **plataforma**: UI para exploración + Batch para generación de artefactos versionados.
- Entregar un “paquete ejecutivo” reproducible: KPIs, hipótesis, evidencias, acciones sugeridas, trazabilidad.

---

## Demo mental: cómo fluye el insight

```mermaid
flowchart LR
  A[NPS térmico
(score + texto)] -->|normaliza| C[(Modelo canónico)]
  B[Helix
(incidencias)] -->|normaliza| C
  C --> D[Mining tópicos + drivers]
  C --> E[Linking semántico
NPS↔Helix]
  D --> F[Hipótesis causales
ranked]
  E --> F
  F --> G[LLM Deep‑Dive Pack
(Markdown + JSON)]
  F --> H[Acciones / Experimentos]
  G --> I[Knowledge Cache
(aprendizaje incremental)]
  I --> F
```

---

## Quickstart

### Requisitos
- **Python 3.9.13** (entorno corporativo)
- `make` (macOS / Linux)
- (Opcional) `xcode-select --install` en macOS para `watchdog` (Streamlit performance)

### Setup
```bash
make setup
```

### Ejecutar UI (Streamlit)
```bash
make run
```

### Ejecutar CI local
```bash
make ci
```

### Ejecutar en modo plataforma (batch)
```bash
make platform CONFIG=configs/batch.json
```

### Build binaria (PyInstaller)
- macOS / Linux (local):
```bash
make build
```

- Windows: vía GitHub Actions (PyInstaller no cross-compila)

---

## Documentación imprescindible (léela en este orden)

1. **Arquitectura** → `ARCHITECTURE.md`  
2. **Módulos del código** → `docs/MODULES.md`  
3. **Contratos de datos (Fuentes y modelo canónico)** → `docs/DATA_CONTRACTS.md`  
4. **Operación y troubleshooting** → `docs/OPERATIONS.md`  
5. **Release y builds** → `docs/RELEASE.md`  
6. **Desarrollo / contribución** → `docs/DEVELOPMENT.md`

---

## Estructura del repo (alto nivel)

- `app/` → UI Streamlit (exploración interactiva)
- `src/nps_lens/` → núcleo (ingesta, analítica, linking, plataforma, LLM packs)
- `tests/` → tests unitarios y de plataforma
- `docs/` → documentación técnica y operativa
- `.github/workflows/` → CI, typecheck, test, builds y release

---

## Principios de diseño (para mantenerlo “nivel empresa”)

- **Contrato de datos**: ingesta validable + normalización + versionado → sin “magia silenciosa”.
- **Trazabilidad**: cada insight debe incluir evidencia cuantitativa y cualitativa y referencias cruzadas.
- **Performance**: caching determinista + pushdown temporal (Año/Mes) + precomputes en ingest.
- **Robustez**: fail‑fast (boot‑check) + writes atómicos + logs accionables.
- **Plataforma**: la UI no “hace magia”; consume casos de uso / servicios del core.

---

## Donaciones

Si te aporta valor (o lo estás usando en producción), puedes apoyar el mantenimiento:

- GitHub Sponsors: https://github.com/sponsors/felixdelbarrio  
- PayPal: https://paypal.me/felixdelbarrio

---

## Licencia

Define la licencia del repo en `LICENSE` (si aplica). En entornos corporativos suele ser privada.

