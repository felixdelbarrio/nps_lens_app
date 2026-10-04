# Mapa de módulos

- `api/app.py`, `cli.py`, `desktop.py`: entradas productivas.
- `settings.py`: configuración de runtime, contexto y preferencias.
- `repositories/sqlite_repository.py`: comentarios, cargas y tablas de taxonomía.
- `core/store.py`: almacenamiento Helix y contexto compartido.
- `core/metrics.py`, `core/nps_math.py`: métricas deterministas.
- `ingest/`: lectura, identidad, fechas, normalización y validación de NPS/Helix.
- `services/taxonomy_service.py`: selección única, artifacts, comparación y snapshots.
- `services/taxonomy_exchange.py`: propuestas y clasificación semántica mediante ZIP.
- `services/helix_exchange.py`: candidatos y decisiones semánticas Helix mediante ZIP.
- `services/classification_protocol.py`, `semantic_validation.py`: contratos y validación.
- `services/dashboard_service.py`: población, evidencia base y proyecciones compartidas.
- `services/analytics/`: KPIs, periodos y narrativas.
- `analytics/linking_policy.py`: política de enlace.
- `analytics/nps_helix_link.py`: retrieval sparse, enlaces y agregados por IDs únicos.
- `analytics/incident_attribution.py`: proyecciones y journeys basados en enlaces válidos.
- `analytics/causal_evidence.py`: evaluación conservadora de asociación observada.
- `analytics/incident_rationale.py`: racional determinista de la evidencia.
- `reports/`: PPT, newsletter, selectores y verificaciones de coherencia.
- `platform/`: publicación, descargas, recursos y previsualización WebApp.
- `domain/`: identidad, modelos de carga, normalización y etiquetas compartidas.
- `ui/`, `design/`: gráficos, narrativas y estilos.

Runtime soportado: Python >=3.12,<3.13, NumPy <2. Consulte `ARCHITECTURE.md`
para los contratos de clasificación activa y evidencia canónica.
