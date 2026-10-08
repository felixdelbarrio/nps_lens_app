# NPS Lens — Plataforma de Insights VoC (NPS + Texto + Incidencias)

`develop`
[![CI](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/ci.yml/badge.svg?branch=develop&event=push)](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/ci.yml?query=branch%3Adevelop)
[![Typecheck](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/typecheck.yml/badge.svg?branch=develop&event=push)](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/typecheck.yml?query=branch%3Adevelop)
[![Test](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/test.yml/badge.svg?branch=develop&event=push)](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/test.yml?query=branch%3Adevelop)

`develop`
[![Release](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/release.yml/badge.svg?branch=develop&event=push)](https://github.com/felixdelbarrio/nps_lens_app/actions/workflows/release.yml?query=branch%3Adevelop)

[![Sponsor](https://img.shields.io/badge/Sponsor-GitHub%20Sponsors-2ea44f.svg)](https://github.com/sponsors/felixdelbarrio)
[![Donate](https://img.shields.io/badge/Donate-PayPal-blue.svg)](https://paypal.me/felixdelbarrio)


---

## Qué es NPS Lens

**NPS Lens** es una plataforma para convertir señales de Voz del Cliente en **insights accionables**, combinando:

- **NPS** (score 0-10 + texto + palanca/subpalanca/canal/segmento)
- **Incidencias Helix** (tickets/bugs) para asociación operativa
- (Opcional) **Reviews** (stores) / **Feedback in‑app** (roadmap)

La aplicación une métricas, verbatims y evidencia trazable; exporta PPT, newsletter y WebApp desde la clasificación activa.

---

## Para qué sirve (valor de negocio)

- Detectar **drivers reales** de detracción (y también de promoción) por palanca/subpalanca/canal.
- Priorizar asociaciones observadas con incidencias y comentarios únicos.
- Construir **journeys de caída** (ruta: palanca → subpalanca → tópico → incidencia → impacto en Score).
- Operar como **plataforma**: UI para exploración y artifacts versionados en SQLite.
- Entregar un “paquete ejecutivo” reproducible: KPIs, hipótesis, evidencias, acciones sugeridas, trazabilidad.

---

## Demo mental: cómo fluye el insight

```mermaid
flowchart LR
  A[NPS
(score + texto)] -->|normaliza| C[(Modelo canónico)]
  B[Helix
(incidencias)] -->|normaliza| C
  C --> D[Mining tópicos + drivers]
  C --> E[Linking semántico
NPS↔Helix]
  D --> F[Evidencia observada]
  E --> F
  F --> H[PPT / newsletter / WebApp]
```

---

## Quickstart

### Requisitos

- **Taxonomy Studio** utiliza tres proyectos GPT mediante archivos: Crea Taxonomía,
  Clasifica taxonomía y Helix Classifier. Sus URLs se editan en la aplicación y se
  guardan al salir del campo. Consulta [el flujo de la iteración 28](docs/iteration28.md).
  En Configuración → ZIP único con progreso y reanudación puedes activar un paquete
  por trabajo. Configura las dos rutas nuevas (vacías por defecto) y copia sus
  instrucciones a los proyectos de comentarios e incidencias. ChatGPT entrega un
  lote completo por turno y espera «Sí» para continuar; importa cada ZIP parcial.
  Conserva el original y las entregas para reanudar en otro chat. Al exportar de
  nuevo, NPS Lens incluye solo pendientes. Desactivado mantiene los ZIP numerados.
  Los criterios, fingerprints y validaciones de importación son iguales en ambos modos.

- **Python 3.12.14** (entorno corporativo)
- `make` (macOS / Linux)
- (Opcional) `xcode-select --install` en macOS para builds nativas.

### Setup
```bash
make setup
```

### Ejecutar UI
```bash
make run
```

### Ejecutar CI local
```bash
make ci
```

### Ejecutar typecheck local
```bash
make typecheck
```

### Ejecutar lint local
```bash
make lint
```

### Build binaria (PyInstaller)
- macOS / Linux (local):
```bash
make build
```

- Windows: vía GitHub Actions (PyInstaller no cross-compila)

---

## Estructura funcional actual

La experiencia operativa se organiza así:

- **OWNER SUPPORT COMPANY**: único contexto transversal para Insights, Ingesta y Datos.
- **PERIOD CONTAINER**: periodo global (`Año`, `Mes`) para toda la app, tablas y reportes.
- **Evolución NPS**: KPIs, evolución temporal y comparativas cruzadas afectados por Service + Period; muestra acumulado hasta el periodo y periodo actual con delta histórico.
- **Comentarios**: filtros sincronizados de `Canal` y `Grupo Score`; contiene comentarios, cambios históricos y brechas.
- **Evidencia Helix ↔ VoC**: conecta incidencias Helix y comentarios NPS mediante el vista de evidencia configurado; usa la atribución opcional N1/N2 del Canal y oculta `Grupo Score`.
- **Reporte ejecutivo**: añade la preferencia `dimensionAnalisis` (`palanca`/`subpalanca`) para decidir si el deck incluye las slides de palanca o subpalanca, con numeración dinámica.

Semántica:

- **Score** es el valor individual 0-10 o su media.
- **NPS clásico** es el índice `% promotores - % detractores`.
- **NPS** se mantiene como nombre de fuente/dominio.

Canal procede del fichero NPS y sus equivalencias configuradas. Los KPIs NPS se calculan siempre sobre todas las opiniones; Canal solo selecciona los tópicos o el ámbito de evidencia. En Evidencia Helix ↔ VoC, una asignación opcional permite vincular valores N1/N2 de Helix a un Canal y, sin asignación, se utilizan todos los comentarios, canales e incidencias de la compañía. Los enlaces Helix se construyen siempre como `base_url + Record ID`.

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

- `frontend/` → UI React (exploración interactiva)
- `src/nps_lens/` → núcleo (ingesta, analítica, linking, plataforma, clasificación semántica)
- `tests/` → tests unitarios y de plataforma
- `docs/` → documentación técnica y operativa
- `.github/workflows/` → `test`, `typecheck`, `ci`, `codeql` y `release` multi-plataforma

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
