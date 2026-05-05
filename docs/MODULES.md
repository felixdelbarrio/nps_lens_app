# Módulos del proyecto (mapa de código)

Este documento explica el **paquete `src/nps_lens/`** y cómo navegarlo sin perderte.

> Regla para contribuciones: si añades un módulo nuevo, actualiza este mapa.

---

## 1) Árbol de paquetes (resumen)

- `nps_lens/`
  - `application/` — orquestación (servicios / casos de uso)
  - `analytics/` — drivers, texto, causalidad, changepoints, linking NPS↔Helix
  - `core/` — stores, caching, perf, profiling, métricas
  - `design/` — tokens/escala de colores (design system)
  - `domain/` — reglas compartidas de negocio: métodos causales, labels Score/NPS y resolución de enlaces Helix
  - `ingest/` — ingesta + normalización + validación
  - `llm/` — contratos/validación y generación de packs
  - `reports/` — generación de presentaciones PPT para comité
  - `models/` — modelos canónicos (Pydantic)
  - `platform/` — ejecución batch + artefactos versionados
  - `services/` — servicios de aplicación y SSOT analítica compartida por API/UI/PPT
  - `quality/` — utilidades de calidad/perf (dev)
  - `ui/` — componentes de UI (charts, theme, narratives, population)

---

## 2) Descripción por módulo (qué hace y qué no)

### `nps_lens.config`
- Carga `.env` y define `Settings`.
- Normaliza listas y mapas (ej. `service_origin_n1_map`).
- **No** debe leer datasets ni tocar frontend.

### `nps_lens.core.store`
- Persistencia de datasets por contexto (Parquet + meta).
- APIs:
  - `save_df(ctx, df, meta=...)`
  - `get(ctx)`
  - `load_table(ctx, cols=..., date_start=..., date_end=..., ...)`
- Incluye subset cache “hot” (best‑effort).

### `nps_lens.core.disk_cache`
- Cache determinista de resultados pesados.
- Escritura atómica (anti‑corrupción).

### `nps_lens.core.perf` / `nps_lens.core.profiling`
- Timers por namespace
- Profiling opcional (cProfile) y resumen

### `nps_lens.ingest.nps_thermal`
- Lectura Excel NPS térmico (openpyxl)
- Normalización de columnas mínimas
- Validación (issues con nivel INFO/WARN/ERROR)

### `nps_lens.ingest.helix_incidents`
- Lectura Excel Helix
- Normalización de fechas (epoch robusto) y columnas (incl. variantes ES/EN)
- Filtrado por contexto (Company/N1/N2) con tolerancia cuando el extract viene filtrado

### `nps_lens.ingest.features`
- Precomputes que reducen CPU:
  - `_service_origin_n2_key` (token-set normalizado)
  - `_text_norm` (texto barato)
- **Regla**: la UI asume que estas features existen (sin retrocompat).

### `nps_lens.analytics.*`
- `drivers.py`: ranking de palancas/subpalancas por impacto/volumen
- `text_mining.py`: tópicos / keywords (MVP)
- `nps_helix_link.py`: linking y agregados NPS↔Helix (diario/semanal)
- `incident_rationale.py`: modelo central de atribución incidencia -> journey -> VoC -> Score (probabilidad de foco, delta Score esperado, impacto total, prioridad y plan de acción)
- `incident_attribution.py`: cadenas causales presentables basadas en links explícitos Helix ↔ VoC con evidencias reutilizables en app, pack y PPT
- `causal.py`: score causal best‑effort con logit / heurísticas
- `changepoints.py`: detección de cambios (ruptures opcional)

### `nps_lens.llm.*`
- `insight_response.py`: schema + validador del JSON de respuesta del LLM
- `knowledge_cache.py`: persistencia y recuperación de aprendizajes
- `pack.py`: generación y export de packs (Markdown + JSON)

### `nps_lens.reports.*`
- `executive_ppt.py`: composición de PPT de negocio (scope, KPI, gráficos, Impact Chain y plan de acción) usando la misma fuente de verdad analítica que la UI

### `nps_lens.services.analytics.*`
- `kpis_service.py`: KPIs de Score, acumulado hasta periodo y deltas vs histórico.
- `nps_service.py`: fachada semántica para lógica NPS compartida.
- `insights_service.py`: textos explicativos de gráficos reutilizables por UI y reporte.
- **Regla**: componentes React y slides no recalculan KPIs; consumen estos servicios a través del backend.

### `nps_lens.services.helix_service`
- `build_helix_url(record_id, base_url=...)`: única función para construir URLs Helix.
- Normaliza la base y concatena `Record ID`; no acepta `Incident Number` como sufijo funcional.

### `nps_lens.services.dashboard_service`
- Fachada de dominio para la app React/FastAPI.
- Centraliza Service Container, Period Container, filtro Canal, Grupo Score, KPIs, payloads de Sumario/Analítica/Linking, tablas y generación PPT.
- El Sumario del Periodo usa solo Service + Period. Analítica NPS Térmico usa Canal + Grupo Score. Incidencias ↔ NPS fija Canal=`Web`, Método causal=`Por Palanca` y usa el histórico completo de incidencias.

### `nps_lens.domain.helix_links`
- Resuelve enlaces Helix desde columnas de incidencia y `Record ID`.
- Delega la construcción de URL en `services.helix_service`.
- Evita construir enlaces con `Incident Number` y no deja que URLs explícitas sobreescriban el `Record ID`.

### `nps_lens.platform.*`
- `batch.py`: ejecución headless según config JSON
- `artifacts.py`: layout de artefactos versionados

### `nps_lens.ui.*`
- `theme.py`: tema tokenizado (light/dark) + CSS quirúrgico
- `population.py`: `Año`/`Mes` del Period Container y window temporal
- `charts.py`: charts y tablas (Plotly)
- `narratives.py`: textos ejecutivos y narrativa causal reusable para UI/PPT
- `components.py`: componentes visuales reutilizables, incluido el bloque `Impact Chain`
- `business.py`: utilidades de slicing y ventanas

---

## 3) Convenciones de nombres (para coherencia)
- Columns canónicas:
  - `Fecha`, `NPS`, `NPS Group`, `Comment`, `Palanca`, `Subpalanca`, `Canal`
- Semántica visual:
  - `Score` para valores o medias 0-10
  - `NPS clásico` para `% promotores - % detractores`
  - `NPS térmico` para fuente/dominio
- Features internas:
  - prefijo `_` (ej. `_service_origin_n2_key`)
- Context keys:
  - `service_origin`, `service_origin_n1`, `service_origin_n2`

---

## 4) Dónde tocar si quieres cambiar X

- **Cambiar contrato de ingest** → `docs/DATA_CONTRACTS.md` + `ingest/*`
- **Cambiar UI (diseño)** → `design/tokens.py` + `ui/theme.py`
- **Cambiar ranking causal** → `analytics/causal.py` + `analytics/nps_helix_link.py`
- **Cambiar plataforma batch** → `platform/batch.py` + `platform/artifacts.py`
