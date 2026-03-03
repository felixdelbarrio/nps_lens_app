# NPS Lens — MVP (Senda · México)

Aplicación en **Python 3.9.13** para análisis avanzado de **NPS térmico** + VoC, con arquitectura **multi-fuente** y extensible a más **geografías** (ES/CO/PE/AR/…) y **canales** (Senda/Gema/…).

Incluye:
- Ingesta + validación + normalización (NPS térmico Excel/CSV, Incidencias, Reviews)
- Drivers / segmentación (gap vs NPS global)
- Detección de cambios (change-points) *(ruptures)*
- Minería de texto (topic clustering TF‑IDF + KMeans; tono pragmático rule-based)
- Journey routes (palanca → subpalanca → topic)
- **WoW**: generación de **LLM Deep‑Dive Pack** (Markdown + JSON) + **Knowledge Cache** (dedup + aprendizaje incremental)
- UI: **Streamlit** (rápido, productivo, portátil)

> **Diseño**: el proyecto incluye las *especificaciones BBVA Experience* y set de iconos/font assets **tal cual** en `design/` y `assets/`.  
> La UI aplica tokens de forma **conservadora** (Streamlit limita theming). No se inventan estilos fuera del sistema.

---

## Quickstart (3–6 pasos)

1) **Crear entorno**
```bash
make setup
```

2) **Ejecutar tests y calidad**
```bash
make ci
```

3) **Lanzar la app**
```bash
make run
```

4) *(Opcional)* Generar un ejemplo de Deep‑Dive Pack por CLI:
```bash
.venv/bin/nps-lens build-example-pack
```

---

## Datos (MVP)

- Muestra de NPS térmico (Senda MX) en:
  - `data/examples/nps_thermal_senda_mx_sample.csv`
- Ejemplos de Incidencias y Reviews:
  - `data/examples/incidents_sample.csv`
  - `data/examples/reviews_sample.csv`

### Ingesta del Excel real
El pipeline está en `src/nps_lens/ingest/nps_thermal.py`.

Ejemplo:
```bash
.venv/bin/nps-lens profile-nps "path/to/NPS Térmico.xlsx" --geo MX --channel Senda
```

---

## Arquitectura (src layout)

- `src/nps_lens/models/`: modelo canónico (Pydantic v1)
- `src/nps_lens/ingest/`: contratos de esquema + normalización por fuente
- `src/nps_lens/quality/`: profiling y reglas de calidad (missing/outliers/duplicados)
- `src/nps_lens/analytics/`: drivers, texto, change-points, causal best-effort, journey
- `src/nps_lens/llm/`: Deep‑Dive Pack + Knowledge Cache
- `src/nps_lens/design/`: tokens (JSON) + capa mínima de estilo
- `app/streamlit_app.py`: UI

---

## Causalidad pragmática (MVP)

`src/nps_lens/analytics/causal.py` implementa un modo **best-effort** basado en:
- Outcome: detractor (NPS <= 6)
- Treatment: pertenecer a un driver (p.ej. `Palanca == X`)
- Controles observables: canal/palanca/subpalanca (y extensible a segmento/periodo/geo)

Salida: `CausalHypothesis` con **supuestos** y **warnings** explícitos.

---

## Journey routes (MVP)

`src/nps_lens/analytics/journey.py` genera rutas:
`palanca → subpalanca → topic`

- Topics: clustering ligero TF‑IDF + KMeans (por corpus)
- Asignación a filas: heurística best‑effort por términos top
- Ranking: concentración de detractores × volumen

---

## WoW — LLM Deep‑Dive Pack + Knowledge Cache

- Exporta pack a:
  - `reports/examples/<id>__pack.md`
  - `reports/examples/<id>__pack.json`
- Cache de conocimiento:
  - `knowledge/insights_cache.json`

Política de deduplicación:
- `signature = sha1(title + context)` estable (ver `stable_signature()`).

La UI permite:
1) Generar pack (copy/paste),
2) Pegar respuesta del LLM,
3) Guardar en cache para:
   - evitar repetir insights,
   - enriquecer explicación futura,
   - registrar decisiones/acciones.

---

## Sistema de diseño (BBVA Experience)

- PDFs de specs: `design/specs_pdf/`
- Tokens extraídos (subset) usados por la app: `design/tokens.json`
- Iconos: `assets/icons/` (extraído de `all_icons.zip`)
- Zips originales: `design/source/`

**Nota**: Las fuentes (Benton/Tiempos) se adjuntan como zips en `assets/fonts/` según el material proporcionado.  
La app **no** las instala automáticamente (para evitar violar políticas internas/licencias); ver README de tu organización para distribución/instalación.

---

## Roadmap (siguiente iteración)

1) Multi-fuente real:
- Conectores corporativos a Incidencias y Stores (Apple/Google), con versionado de datasets.
2) Linking (EvidenceLinks):
- similitud semántica + heurísticas por ventana temporal/geo/canal/palanca.
3) Scoring causal:
- temporalidad + consistencia por cohortes + dose-response (si hay datos).
4) Journey graph:
- grafo completo con pesos y rutas emergentes.
5) UI avanzada:
- vistas comparativas multi-geo/canal, alerts, export a PowerPoint.

---

## Licencia / Uso
Uso interno. Ajusta `pyproject.toml` según tu política corporativa.
