# Refactor hacia evidencia objetiva

## Estado local

El Taxonomy Engine anterior está terminado y verificado. La siguiente iteración se ha
iniciado con el bloque de Brechas NPS, cerrado de extremo a extremo. El refactor global
hacia evidencia objetiva **todavía no está terminado**.

## Bloque terminado: Brechas NPS

Se elimina `analytics/opportunities.py`, el uplift del 60 % del gap, la confianza derivada
del volumen y la ordenación mediante su producto. No se conservan aliases.
`analytics/drivers.py` calcula una sola tabla de estadísticas observadas; `nps_gaps.py`
selecciona sus brechas negativas por diferencia, volumen y etiqueta, sin combinarlos.

El dashboard usa su contrato `gaps`, sin un segundo bloque `opportunities`. Expone NPS,
brecha firmada (NPS del grupo menos global), respuestas, notas válidas, detractores y
peso sobre todas las filas del ámbito. El NPS excluye notas ausentes; el volumen de
respuestas sí las incluye. Los grupos sin etiqueta no entran en la selección de brechas
para el informe; sus respuestas sí forman parte del global y de la muestra.

Los textos del informe, los packs batch y el resumen de la newsletter describen brechas,
sin recomendaciones automáticas por uplift. Se elimina el cálculo y los campos de
presentación de oportunidades del PPT que ya no se renderizaban. La navegación local
y WebApp se denomina «Brechas NPS».

El ajuste se llama `min_n_nps_gaps` / `NPS_LENS_UI_MIN_N_NPS_GAPS`. El antiguo ajuste
se retira; si se había personalizado, debe configurarse con el nuevo nombre.
Los snapshots nuevos usan el contrato actualizado; no se reescriben publicaciones previas.

## Auditoría y trabajo pendiente

Se buscaron los términos del encargo y sus consumidores antes de implementar.
El mayor volumen de referencias se concentra en:

- `analytics/incident_rationale.py`: eliminar riesgo, recuperación, confianza, score
  combinado, prioridad y plan de acción. Conservar comparaciones de periodos con
  denominadores y criterios de partición explícitos; no llamar NPS a una media 0–10.
- `analytics/incident_attribution.py`: retirar propagación y selección por esos campos,
  labels prefijados de impacto/confianza y relatos que afirman causalidad. Mantener
  vínculos, ejemplos, similitud y volúmenes; evitar duplicar respuestas al agregarlos.
- `analytics/helix_operational_metrics.py`: conservar organizaciones y duraciones
  observadas, renombrar ETA histórico y eliminar responsables/plazos inferidos.
- `services/dashboard_service.py`: retirar enriquecimiento heurístico y schemas de
  escenarios, actualizar resumen, gráficos, exportación y snapshots en conjunto.
- `ui/narratives.py`, `ui/charts.py`, `reports/executive_ppt.py` y
  `reports/content_selectors.py`: todavía contienen métricas y copy causal del modelo
  anterior; deben migrarse junto con frontend y WebApp, sin limitarse a ocultar campos.
- Actualizar pruebas de causalidad, Helix, PPT y API para que los payloads completos y
  textos no contengan las métricas eliminadas.

## Clasificación de referencias

- Datos observados: NPS, recuentos, proporciones, ejemplos, organización responsable
  de Helix y fechas/duración de resolución. Conservar con denominadores y ámbito claros.
- Estadísticas reproducibles: similitud, correlación, lag, changepoints y estabilidad,
  diferencias entre periodos/grupos. Conservar; renombrar probabilidad/cambio esperado
  cuando describan tasas y diferencias observadas.
- Heurísticas: pesos de confianza/causalidad/prioridad, recuperación multiplicada por
  constantes, uplift y recomendaciones/owner/ETA derivados. Eliminar cálculo y consumo.
- La confianza predictiva de COMPLETED pertenece al clasificador supervisado evaluado
  con holdout y Macro-F1. No es la confianza causal heurística; no eliminarla por una
  sustitución global de palabras. Tampoco borrar variables `expected` de tests o del
  parser cuando representen un valor esperado de una aserción o esquema.
