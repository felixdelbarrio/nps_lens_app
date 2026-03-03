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

> **Diseño**: la app sigue el *patrón de tokens BBVA Experience* con estilos **centralizados**.
> - Motor de tema (light/dark) y CSS variables: `src/nps_lens/ui/theme.py`
> - Tokens (subset) en código: `src/nps_lens/design/tokens.py`
> No se distribuyen PDFs, tipografías ni packs de iconos en el repo.

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

## UI (orientada a negocio)

Páginas principales:

- **Resumen ejecutivo**: KPIs, tendencia y un **informe ejecutivo** listo para copiar/pegar.
- **Comparativas**: periodo actual vs periodo base (qué cambió y dónde).
- **Cohortes**: matriz por segmento/usuario para localizar bolsas de fricción.
- **Drivers & oportunidades**: priorización por impacto + confianza.
- **Texto & temas**: qué se repite y cómo suena (lenguaje natural).

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
- `src/nps_lens/design/`: tokens (subset) centralizados en código
- `src/nps_lens/ui/`: theme (light/dark), componentes y gráficos
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

Esta versión es **design-token-first**:

- Tokens (subset) y mapeo semántico centralizado: `src/nps_lens/design/tokens.py`
- Tema Light/Dark + CSS variables centralizadas: `src/nps_lens/ui/theme.py`
- Componentes UI (cards/KPIs/sections): `src/nps_lens/ui/components.py`

No se incluyen assets propietarios (PDFs, tipografías o packs de iconos) dentro del repo.
Si tu organización tiene el paquete oficial, reemplaza únicamente los valores en `tokens.py`.

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


**Nota**: el soporte Excel (.xlsx) requiere `openpyxl` y ya viene incluido en dependencias.
