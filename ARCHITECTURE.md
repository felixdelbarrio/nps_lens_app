# Arquitectura NPS Lens

Runtime y tooling: Python >=3.12,<3.13; NumPy <2. React consume FastAPI.
Los entrypoints son `nps_lens.cli`, `nps_lens.desktop` y `api.app:create_app`.

## Datos y clasificación activa

`SqliteNpsRepository` conserva los comentarios con `_business_key`; `HelixIncidentStore`
conserva las incidencias. `TaxonomyService.resolve()` selecciona las etiquetas antes
 de calcular resultados. Los modos reales son SOURCE, COMPLETED y DISCOVERED;
las equivalencias normalizan SOURCE y COMPLETED sin añadir otro modo.

Las asignaciones se materializan en `taxonomy_artifacts`, identificadas por contenido,
modo, motor e input. Cambiar `comment_engine` selecciona otra clasificación; las versiones
anteriores permanecen en SQLite. Los lotes LLM usan firmas que incluyen sus asignaciones.
Los snapshots congelan etiquetas, canales e identidad de clasificación y se restauran
sin ejecutar modelos. Las comparaciones explícitas pueden mostrar varias lentes.

## Evidencia Helix ↔ VoC

`DashboardService._causal_analysis_bundle()` conserva una evidencia base por datos,
clasificación activa, motor, ámbito y política. Las vistas Palanca, Subpalanca, Source N2
y Journey proyectan esos mismos enlaces. `link_incidents_to_nps_topics()` utiliza
TF-IDF sparse por bloques y top-K determinista, sin muestreo global de comentarios.
`analytics/linking_policy.py` contiene los valores de la política.

El texto real valida el vínculo; la etiqueta de taxonomía sola no lo valida.
Los enlaces contienen IDs de comentario/incidencia, fechas, firma de clasificación,
similitud de verbatim y evidencia ID. La similitud de taxonomía no se estima y queda
sin valor. Los agregados cuentan IDs únicos por tópico. Journeys agrupa recorridos
de la taxonomía activa; no entrena un segundo TF-IDF. El ranking prioriza incidencias
únicas, comentarios únicos y después calidad de evidencia.

## Hooks LLM

`TaxonomyExchange` y `HelixExchange` exportan/importan archivos con contratos estrictos,
hashes, versiones de instrucciones y evidencia validada. Helix recibe candidatos
reducidos por retrieval. No hay cliente API automático: el modelo externo no se puede
verificar desde la aplicación. Python calcula volúmenes, NPS, ventanas y rankings.
El LLM aporta decisiones semánticas materializadas; no una segunda verdad cuantitativa.

## Salidas

Dashboard, PPT y newsletter consumen los servicios y selectores compartidos.
La WebApp publica resultados ya calculados. Los enlaces a tickets se construyen con
`base_url + Record ID`. Las salidas describen asociación observada, no causalidad probada.
