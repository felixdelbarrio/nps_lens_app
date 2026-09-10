# Refactor hacia evidencia objetiva

## Estado local

El Taxonomy Engine y el refactor global hacia evidencia objetiva están terminados y
verificados en local. Dashboard, Causalidad, informes, PPT y snapshots comparten el
mismo contrato descriptivo.

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

## Causalidad descriptiva

`analytics/incident_rationale.py` compara semanas de incidencia baja y alta con medias
ponderadas por respuestas. Publica recuentos, tasas, diferencias observadas,
correlación, lag y estabilidad de changepoints. No calcula un score único de causalidad.

Los escenarios se ordenan de forma lexicográfica por vínculos semánticos, incidencias,
comentarios, respuestas y similitud. La ficha conserva los casos Helix y VoC, el
heatmap, los changepoints y el lag para que cada asociación pueda revisarse.

La información operativa muestra las organizaciones responsables presentes en Helix y
la duración media histórica de resolución. No infiere responsables ni plazos.

Los relatos y PPT describen observaciones, comparaciones y asociaciones. La WebApp lee
esos mismos datos del snapshot y no recalcula métricas.

## Clasificación de referencias

- Datos observados: NPS, recuentos, proporciones, ejemplos, organización responsable
  de Helix y fechas/duración de resolución. Conservar con denominadores y ámbito claros.
- Estadísticas reproducibles: similitud, correlación, lag, changepoints y estabilidad,
  diferencias entre periodos/grupos. Conservar; renombrar probabilidad/cambio esperado
  cuando describan tasas y diferencias observadas.
- Heurísticas: pesos de confianza/causalidad/prioridad, recuperación multiplicada por
  constantes, uplift y recomendaciones/owner/ETA derivados. Eliminar cálculo y consumo.
- La certeza de asignación de COMPLETED pertenece al clasificador supervisado evaluado
  con holdout y Macro-F1. Se publica como `assignment_certainty` y se configura mediante
  `certainty_threshold`, sin mezclarla con causalidad.
