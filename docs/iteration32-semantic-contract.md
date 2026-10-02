# Iteración 32: contrato semántico y retrieval Helix

## Diagnóstico y alcance

El flujo existente ya resolvía los IDs de categoría de forma conjunta, verificaba
manifiestos y conservaba huellas individuales. Sin embargo, el catálogo diseñado
solo tenía nombres, se pedían evidencias/razones por clasificación, Helix repetía el
corpus NPS completo y exigía igualdad de categorías para aceptar vínculos. El texto
orientado a linking descartaba campos desconocidos y truncaba narrativas: podía
perder comprobaciones o negaciones.

## Contratos y decisiones

- `nps-lens-taxonomy/4`: cada Subpalanca diseñada contiene `name` y `criterion`
  obligatorio, de hasta 500 caracteres. Se conserva la revisión del diseñador,
  que tiene consumidor en el artefacto persistido; no se pide una llamada adicional.
- `nps-lens-comments/5`: únicamente `id`, `primary`, `secondary`. Se eliminan DTO,
  validación y persistencia de evidence/reason por comentario.
- `nps-lens-helix/5`: clasificación por IDs y `links`, con citas de ambas fuentes
  y confianza. Se eliminan evidence, reason y rationale/llm_rationale, sin consumidor
  funcional. Cada incidencia contiene únicamente sus candidatos recuperados.
- `taxonomy_fingerprint` es el hash del catálogo canónico ordenado que incluye
  Palanca, Subpalanca y criterion. Se guarda tras diseñar, en las asignaciones NPS,
  en el manifiesto Helix y en las clasificaciones persistidas de incidencias.
  Las importaciones rechazan manifiestos o taxonomías incompatibles.
- Original y Manual conservan su contrato de edición de etiquetas. Su catálogo LLM
  aplica una política explícita y centralizada de significado literal de esas
  etiquetas. No se inventan definiciones de dominio ni se convierten taxonomías
  diseñadas antiguas. Copiar una taxonomía al editor Manual conserva la proyección
  de etiquetas propia de ese editor; la taxonomía diseñada mantiene sus criterios.
- El resolver limpia ambos valores de una pareja LLM parcial; no modifica la regla
  histórica de SOURCE. La clasificación compacta resuelve ambos valores del mismo ID.
- `retrieve_incident_candidates` concentra el TF-IDF, vocabulario, filtros de fechas,
  umbral y Top-K existentes. Analytics y exportación Helix llaman a esa misma función.
  El desempate usa el ID del comentario. El cálculo es agregado por exportación;
  no se añaden cachés persistentes, motores de matching ni embeddings.
- Un vínculo importado debe proceder de los candidatos de esa incidencia y conservar
  sus fuentes. La selección temporal se garantiza al recuperar; cambios posteriores
  en las fechas/textos invalidan la respuesta. Categorías diferentes no impiden el link.
- El texto funcional omite campos administrativos identificados y conserva campos
  desconocidos, narrativas, descartes y negaciones sin cortes arbitrarios de longitud.
  Los límites existentes de bytes siguen rechazando entradas excesivas explícitamente.
- Se mantiene el fan-out de duplicados exactos. La normalización conservadora opcional
  queda fuera de esta entrega.

## Archivos de implementación

- `src/nps_lens/services/taxonomy_prompts.py`
- `src/nps_lens/services/taxonomy_discovery.py`
- `src/nps_lens/services/classification_protocol.py`
- `src/nps_lens/services/taxonomy_exchange.py`
- `src/nps_lens/services/helix_exchange.py`
- `src/nps_lens/services/taxonomy_service.py`
- `src/nps_lens/services/semantic_validation.py`
- `src/nps_lens/analytics/nps_helix_link.py`

No se cambia el código de frontend ni la WebApp publicada. Solo se actualiza el
constructor de respuestas simuladas del E2E para el nuevo protocolo. No se añaden
llamadas LLM, evaluadores con tokens, ni compatibilidad con schemas sustituidos.
Las taxonomías diseñadas y respuestas de intercambio anteriores deben regenerarse.
No se migran ni alteran bases de datos del usuario durante esta implementación.

## Verificación

Las pruebas usan datos temporales y respuestas simuladas, nunca llamadas reales al
LLM. `tests/test_iteration32_semantics.py` cubre criterios, huellas estables, resolución
atómica, SOURCE parcial, Top-K/fechas compartidos, candidatos y negaciones. Se
actualizan las regresiones de intercambio, frameworks, reinicio y el E2E existente.
Las pruebas de código garantizan contratos; no demuestran por sí solas la calidad
semántica de un modelo ante textos reales.
