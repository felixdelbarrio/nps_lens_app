# ARCHITECTURE — NPS Lens (NPS ↔ Helix)

Este documento describe la arquitectura **end‑to‑end** de NPS Lens: ingesta, modelo canónico, analítica, linking multi‑fuente, generación de artefactos, UI y ejecución batch (plataforma).

---

## 1) Visión general (componentes)

```mermaid
flowchart TB
  subgraph UI[UI (React + FastAPI)]
    S1[Sidebar: SERVICE CONTAINER
BUUG/N1/N2]
    SPeriod[Sidebar: PERIOD CONTAINER
Año/Mes]
    S2[Tabs: Sumario del Periodo · Analítica NPS Térmico · Incidencias↔NPS · Datos]
  end

  subgraph Core[Core / Plataforma]
    Cfg[Settings (.env)]
    Store[DatasetStore + HelixIncidentStore
Parquet + Meta + Subset cache]
    Ingest[Ingest
NPS + Helix + (Reviews)]
    Feat[Precompute Features
(_service_origin_n2_key, _text_norm, ...)]
    Cache[DiskCache
(results, deterministic, atomic)]
    Perf[PerfTracker + Profiling
(optional)]
    Analytics[Analytics services
KPIs · Insights · Drivers · Topics · Causal]
    Link[Linking
TF‑IDF + cosine
EvidenceLinks]
    Pack[LLM Deep‑Dive Pack
Markdown + JSON
schema versionado]
    KCache[Knowledge Cache
learning factor]
    Batch[Platform Batch Runner
artifacts/]
  end

  S1 --> Cfg
  SPeriod --> Cfg
  UI --> Store
  UI --> Analytics
  UI --> Link
  UI --> Pack
  Batch --> Store
  Batch --> Analytics
  Batch --> Link
  Batch --> Pack
  Pack --> KCache
  KCache --> Analytics
  Ingest --> Store
  Ingest --> Feat --> Store
  Analytics --> Cache
  Link --> Cache
  Store --> Cache
  Perf --> Analytics
```

---

## 2) Modelo de ejecución

### 2.1 UI (exploración)
- El usuario selecciona **SERVICE CONTAINER**: `service_origin` + `service_origin_n1` + (opcional) `service_origin_n2`.
- Selecciona **PERIOD CONTAINER**: `Año` / `Mes`. Es transversal a dashboard, tablas y reportes.
- `Canal` y `Grupo Score` viven bajo **Analítica NPS Térmico**. En **Incidencias ↔ NPS**, el canal queda fijado a `Web`, el grupo Score se elimina de la UI y `Método causal` queda por defecto en `Por Palanca`.
- **Sumario del Periodo** no aplica Canal ni Grupo Score: sus KPIs y gráficos dependen solo de Service + Period.
- La UI consume payloads de `DashboardService`; no calcula KPIs, periodos, grupos ni URLs Helix.
- Los KPIs, deltas de ámbito, narrativas de gráficos y URLs Helix viven en `src/nps_lens/services/analytics/*` y `src/nps_lens/services/helix_service.py`, compartidos por UI, API y generación PPT.

**Regla de oro**: la UI no “inventa” lógica; orquesta casos de uso del core y renderiza resultados.

### 2.2 Semántica Score/NPS
- **Score** = valor individual 0-10 y medias 0-10.
- **NPS clásico** = `% promotores - % detractores`.
- **NPS térmico** = fuente/dominio; las columnas históricas `NPS` y `NPS Group` se conservan por contrato de ingesta.
- Las etiquetas visuales y de PPT usan `Score medio`, `Score actual`, `Score base` o `Delta Score` cuando hablan de medias 0-10.

### 2.3 Batch (plataforma)
- Ejecuta specs (JSON) de múltiples contextos.
- Genera artefactos versionados en `artifacts/<dataset_id>/<pipeline_version>/<ctx_sig>/...`.

---

## 3) Contratos de datos (resumen)
Ver `docs/DATA_CONTRACTS.md` para detalle de columnas, normalización y esquema canónico.

---

## 4) Caching, performance y robustez

### 4.1 DiskCache determinista
- Key derivada de: dataset signature + params + namespace.
- Escritura atómica: `tmp -> replace()` para evitar corrupción.

### 4.2 Pushdown temporal (Año/Mes)
- Preferencia por filtrar en lectura Parquet (col `Fecha_day`) para reducir RAM/CPU.
- Caso especial: Año=Todos y Mes!=Todos → filtro post‑load (no hay rango continuo).

### 4.3 Fail‑fast / boot‑check
En arranque se valida:
- UI primitives (constantes y mappings)
- tokens de tema completos
- schema mínimo del dataset persistido

---

## 5) Linking multi‑fuente

### 5.1 Objetivo
Enlazar verbatims detractores ↔ incidencias Helix con evidencia trazable.

Los enlaces Helix se resuelven en una única capa de dominio: se buscan identificadores de incidencia (`Incident Number`, `ID de la Incidencia`, `id`) y la URL final se construye con `buildHelixUrl(recordId)`, equivalente a `base_url + Record ID`/`workItemId`/`InstanceId`. No se usa `Incident Number` como sufijo de URL ni se priorizan URLs explícitas frente al `Record ID`.

### 5.2 Implementación MVP
- Limpieza de texto + TF‑IDF + cosine similarity
- Umbral configurable por UI
- EvidenceLink: `nps_id`, `incident_id`, `similarity`, explicación

---

## 6) Causalidad pragmática
Rankeo de hipótesis por:
- temporalidad (incidencias preceden cambios)
- fuerza (delta rate / lifts)
- consistencia (cohortes/periodos)
- plausibilidad semántica (text linking)
- señal de changepoints / lag (si aplica)

---

## 7) Artefactos (Deep‑Dive Pack)

Genera:
- Markdown (copy/paste)
- JSON (contract versionado)

Incluye:
- resumen ejecutivo
- hipótesis y evidencia
- ejemplos cualitativos
- acciones sugeridas
- trazabilidad técnica (filtros, versión)

---

## 8) Seguridad y privacidad (mínimo)
- No persistir PII innecesaria.
- Evitar dumps de datasets en logs.
- Guardar tokens/cookies fuera del repo (`.env`).

---

## 9) Rutas recomendadas de evolución
- Multi‑fuente completa (reviews + in‑app)
- Embeddings para linking más robusto
- API (FastAPI) para servir artefactos y KPIs
- Scheduler/registry (Airflow/Jenkins) para batch recurrente
