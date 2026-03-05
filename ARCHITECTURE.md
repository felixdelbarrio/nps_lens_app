# ARCHITECTURE — NPS Lens (NPS ↔ Helix) ⚙️

Este documento describe la arquitectura **end‑to‑end** de NPS Lens: ingesta, modelo canónico, analítica, linking multi‑fuente, generación de artefactos, UI y ejecución batch (plataforma).

---

## 1) Visión general (componentes)

```mermaid
flowchart TB
  subgraph UI[UI (Streamlit)]
    S1[Sidebar: Contexto + Población
(Año/Mes/Grupo)]
    S2[Tabs: Resumen · Drivers · Texto · Journey · Alertas · NPS↔Helix · LLM · Datos]
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
    Analytics[Analytics
Drivers · Topics · Journey · Changepoints · Causal]
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
- El usuario selecciona **contexto**: `service_origin` + `service_origin_n1` + (opcional) `service_origin_n2`.
- Selecciona **población global** (transversal): `Año` / `Mes` / `Grupo NPS`.
- La UI carga dataset desde `DatasetStore` con **pushdown temporal** cuando hay rango continuo (Año + Mes), y aplica el filtro de grupo.

**Regla de oro**: la UI no “inventa” lógica; orquesta casos de uso del core y renderiza resultados.

### 2.2 Batch (plataforma)
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

