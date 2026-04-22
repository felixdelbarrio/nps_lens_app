# Causal Method Coherence Audit

## Scope

This note documents the current `Incidencias ↔ NPS` architecture after the causal-method refactor.

Primary implementation:
- Backend orchestration: `src/nps_lens/services/dashboard_service.py`
- Causal semantics: `src/nps_lens/domain/causal_methods.py`
- Causal attribution: `src/nps_lens/analytics/incident_attribution.py`
- API contract: `src/nps_lens/api/app.py`, `src/nps_lens/api/schemas.py`
- React rendering: `frontend/src/components/LinkingWorkspace.tsx`
- Executive PPT: `src/nps_lens/reports/executive_ppt.py`

## Result

The active causal method is now the single narrative driver for the full workspace and the PPT.

Supported methods:
- `Por Palanca`
- `Por Subpalanca`
- `Helix: Source Service N2`
- `Journeys rotos`
- `Journeys de detracción`

For every method, the backend centralizes:
- label
- summary
- flow
- section titles
- section subtitles
- entity naming
- table naming
- chart naming
- deep-dive focus
- PPT wording

## Canonical Workspace Model

`/api/dashboard/linking` now returns one canonical payload with these top-level sections:
- `causal_method`
- `navigation`
- `situation`
- `entity_summary`
- `scenarios`
- `deep_dive`

The semantic mapping is:
- `Situación del periodo`
  Keeps the main timeline and adds method-aware subtitle, KPIs, metadata and explanatory note.
- `entity_summary`
  Replaces the old fixed `Journeys rotos` navigation with the method-dependent navigation:
  - `Touchpoints afectados por Palanca`
  - `Touchpoints afectados por Subpalanca`
  - `Incidencias por Source Service N2 de Hélix`
  - `Journeys rotos`
  - `Journeys de detracción`
- `scenarios`
  Preserves the layout but rebuilds banner, pills, spotlight, flow and evidence under the active method.
- `deep_dive`
  Replaces `Mapa causal priorizado` as `NPS deep dive` and nests:
  - `Ranking de hipótesis`
  - `Evidence wall`
  - `Data deepdive analysis`

## Backend Responsibilities

The backend owns all heavy work:
- Helix↔VoC linking
- method-aware remapping from topic to causal entity
- aggregation for KPIs, charts and tables
- scenario construction
- deep-dive filtering base
- PPT narrative inputs

React only owns:
- local tab state
- local scenario navigation
- local topic filter selection
- rendering

## Key Refactor Points

1. `src/nps_lens/domain/causal_methods.py`
   Defines the explicit causal-method semantics used everywhere else.

2. `src/nps_lens/analytics/incident_attribution.py`
   Builds method-aware chains and shared remapping helpers for causal entities.

3. `src/nps_lens/services/dashboard_service.py`
   Assembles the canonical linking payload and reuses the same semantics for the PPT.

4. `src/nps_lens/reports/executive_ppt.py`
   Consumes the same causal semantics for slide 7 (`Situación del periodo`), slide 8 (method-aware entity summary) and scenario slides.

5. `frontend/src/components/LinkingWorkspace.tsx`
   Renders the canonical payload without recomputing business semantics.

## Legacy Removed

The refactor removed the old first-level split across:
- `touchpoint_mode`
- `journeys`
- `Mapa causal priorizado`
- top-level `Data deep dive analysis`

Those concepts are now represented inside the canonical model instead of as disconnected payload branches.

## Validation

Useful commands for this area:
- `./.venv/bin/pytest tests/api/test_dashboard_api.py tests/test_business_ppt.py tests/test_incident_attribution.py -q --no-cov`
- `npm --prefix frontend test -- --run`
- `npm --prefix frontend run build`
