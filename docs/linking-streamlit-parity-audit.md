# Streamlit -> React Parity Audit

## Scope

This audit covers the `Incidencias ↔ NPS` workspace restored from the Streamlit source of truth into React.

Original source of truth:
- `app/streamlit_app.py` in commit `b7f704c0ec9f775c0097819b6df5fca071895324`

Current restored implementation:
- Backend orchestration: `src/nps_lens/services/dashboard_service.py`
- API contract: `src/nps_lens/api/app.py`, `src/nps_lens/api/schemas.py`
- React rendering: `frontend/src/components/LinkingWorkspace.tsx`

## Gap Analysis

### Before

1. `Situación del periodo` was reduced to a single timeline figure.
2. `Tópicos trending`, `Ranking de hipótesis` and `Evidence wall` were not rendered with Streamlit parity.
3. `Journeys rotos` was reduced to a generic route table instead of the detected-broken-journeys chart and table.
4. `Análisis de escenarios causales` lost the executive narrative banner, active-chain navigation, causal spotlight card and evidence/detail tabs.
5. The active causal method (`touchpoint_source`) was not sent to `/api/dashboard/linking`, so React could not mirror Streamlit mode-dependent behavior.
6. Linking calculations were partially duplicated or simplified between UI and backend.

### After

1. `Situación del periodo` is restored with:
   - KPI row
   - `Timeline causal (diario)`
   - `Mapa causal priorizado`
   - `Tópicos trending`
   - `Ranking de hipótesis`
   - `Evidence wall`
2. `Journeys rotos` is restored with:
   - KPI row
   - broken journeys bar chart
   - broken journeys detail table
3. `Análisis de escenarios causales` is restored with:
   - narrative banner
   - causal method metrics
   - active chain navigation
   - spotlight card
   - `Cards` / `Tabla`
   - evidence/detail tabs:
     - `Evidencia Helix`
     - `Voz del cliente`
     - `Matriz visual`
     - `Ficha cuantitativa`
     - `Heat map`
     - `Changepoints + lag`
     - `Lag en días`
4. `Data deep dive analysis` is preserved as the requested fourth navigation.
5. Touchpoint mode now drives the linking payload end-to-end.
6. The backend now assembles one canonical linking model and React remains presentation-oriented.

## Traceability

| Restored feature | Streamlit source | New backend equivalent | New React equivalent |
| --- | --- | --- | --- |
| Tab shell and causal workspace navigation | `app/streamlit_app.py:3898-3900` | `DashboardService.linking_dashboard` | `frontend/src/components/LinkingWorkspace.tsx` |
| `Situación del periodo` KPI row + timeline | `app/streamlit_app.py:3905-3969` | `DashboardService._build_linking_overview_figure` | `LinkingWorkspace` situation section |
| `Mapa causal priorizado` KPIs | `app/streamlit_app.py:4227-4235` | `DashboardService.linking_dashboard` summary payload | `LinkingWorkspace` situation KPI row |
| `Tópicos trending` | `app/streamlit_app.py:4145-4242` | `DashboardService._build_topics_trending_figure` | `LinkingWorkspace` situation chart |
| `Ranking de hipótesis` | `app/streamlit_app.py:3970-4294` | `DashboardService.linking_dashboard` ranking assembly with knowledge-cache adjustments | `LinkingWorkspace` situation table |
| `Evidence wall` | `app/streamlit_app.py:4295-4350` | `DashboardService._build_linking_evidence_table` + filtered leader-topic view | `LinkingWorkspace` situation table |
| `Journeys rotos` chart and table | `app/streamlit_app.py:4352-4418` | `DashboardService._build_touchpoint_mode_payload` + `journeys` payload | `LinkingWorkspace` journeys section |
| Narrative banner | `app/streamlit_app.py:4420-4472` | `DashboardService.linking_dashboard` `scenarios.banner` payload | `LinkingWorkspace` scenarios hero |
| Active chain carousel | `app/streamlit_app.py:4473-4595` | `_annotate_chain_candidates`, `_select_chain_rows`, `_cap_chain_evidence_rows` and scenario-card serialization | `LinkingWorkspace` scenarios navigation |
| Spotlight card | `src/nps_lens/ui/components.py:520-593` | `DashboardService._build_linking_scenario_cards` | `LinkingWorkspace` spotlight section |
| Matrix/detail/heat/lag tabs | `app/streamlit_app.py:4597-4769` | `DashboardService._build_linking_scenario_cards`, `_build_linking_detail_table`, `_build_changepoints_lag_figure` | `LinkingWorkspace` scenario detail tabs |
| Touchpoint-mode remapping | `app/streamlit_app.py:3694-3726` | `DashboardService._build_touchpoint_mode_payload` | Driven by `/api/dashboard/linking?touchpoint_source=...` |

## Centralized Data Architecture

Canonical data path for `Incidencias ↔ NPS`:

1. `_compute_linking_core(...)`
   - exact Helix↔VoC linking
   - weekly/daily aggregates
   - base rationale inputs
2. `_build_touchpoint_mode_payload(...)`
   - central remapping for `Subpalanca`, `Journeys rotos` and other causal modes
3. `linking_dashboard(...)`
   - assembles the single source of truth payload for:
   - situation
   - broken journeys
   - causal scenarios
   - deep dive evidence
4. `LinkingWorkspace.tsx`
   - only keeps local UI state:
   - active tab
   - active chain index
   - cards/table toggle
   - deep dive filters

No heavy calculation now lives in React.

## Dead / Obsolete Code Removed

1. Removed legacy `build_routes(...)` usage from `DashboardService._compute_linking_core`.
2. Replaced the simplified inline linking renderer in `frontend/src/App.tsx` with a dedicated workspace component.
3. Removed duplicated deep-dive filtering/sorting state from `App.tsx`; it now lives next to the linking workspace that consumes it.

## Validation

- `python3 -m py_compile src/nps_lens/services/dashboard_service.py src/nps_lens/api/app.py src/nps_lens/api/schemas.py`
- `./.venv/bin/pytest --override-ini addopts='' tests/api/test_dashboard_api.py -q`
- `npm run build`
- `npm run test`
