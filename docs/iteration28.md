# Iteración 28 — Taxonomías y proyectos GPT

Taxonomy Studio contiene tres lentes: **Original**, **Manual** y **Descubierta por LLM**.
El único selector «Lente activa» controla la analítica. Las equivalencias explícitas se
aplican automáticamente a Original y Manual; los datos fuente permanecen intactos.

## Taxonomía

- Original permite explorar las categorías del fichero, cuando existen.
- Manual contiene la tabla de equivalencias y el editor de Palancas/Subpalancas.
  Parte de Original o, si falta, de la taxonomía descubierta. Los renombrados conservan
  las asignaciones; las categorías nuevas no asignan comentarios por sí solas.
- Descubierta por LLM separa los proyectos **Crea Taxonomía** y **Clasifica taxonomía**.
  Cada uno tiene URL, instrucciones, exportación e importación propias.

Las tres URLs GPT se guardan al salir del campo o pulsar Intro, sin botón adicional.
Los enlaces abren los proyectos con la URL guardada. Las instrucciones de «Copiar»,
«Ver» y los archivos exportados proceden de la misma fuente.

## Intercambio NPS

1. En Crea Taxonomía, exportar el ZIP de comentarios y procesarlo en el proyecto GPT.
2. Importar un JSON estricto `{"taxonomy":[{"lever":"…","sublevers":["…"]}]}`.
   Esta acción solo guarda categorías; no inicia otra exportación.
3. En Clasifica taxonomía, exportar los comentarios pendientes y la taxonomía guardada.
4. Importar JSON estricto con `manifest` (copia exacta del manifiesto exportado) y
   `results`, cuyas claves son los IDs de lote. Cada lote contiene `classifications`
   con `id` y `primary_classification: {lever, sublever}`.

Se validan IDs, orden, categorías, corpus y manifiesto antes de escribir. Se admiten
lotes completos parciales y reimportaciones idénticas. Las exportaciones posteriores
omiten los comentarios ya clasificados para ese texto y taxonomía. Una taxonomía nueva
invalida las asignaciones del catálogo anterior. Los proyectos NPS no admiten ZIP de respuesta.

## Helix y Causalidad

Las casillas de las tres taxonomías permiten incluir una, dos o tres en Helix. Esta
selección determina qué clasificaciones se solicitan; no cambia la lente activa.
Helix Classifier dispone de URL editable y exportación/importación ZIP, sin selectores
adicionales de taxonomía ni de método causal.

El protocolo `nps-lens-helix/2` exporta `taxonomies.json`, comentarios con sus categorías
en cada lente y las incidencias con `pending_taxonomies`. Cada resultado contiene
`id` y `assignments`: una pareja Palanca/Subpalanca, explicación y vínculos NPS por
cada taxonomía pendiente. Se guarda cada asignación de forma independiente. Cambiar
Manual no invalida las clasificaciones de Original o Descubierta.

En **Insights → Causalidad** se eligen el método causal y el motor (reglas o LLM).
LLM requiere clasificaciones Helix completas y vigentes para la lente activa.
Cambiar de método reutiliza esas clasificaciones; los agrupamientos y cálculos existentes
siguen siendo comunes. Cambiar textos o categorías exige reclasificar lo afectado.
La confianza semántica no constituye una prueba de causalidad.

## Alcance técnico

Se eliminan las lentes normalizadas separadas, la generación ML local obsoleta y el
protocolo Helix dependiente del método. Los intercambios tienen límites de tamaño y
retención: tres trabajos por contexto y nueve ámbitos de clasificaciones Helix. Los
ZIP nunca se extraen ni ejecutan. Las operaciones de edición e intercambio siguen
limitadas a la aplicación local. La WebApp estática conserva su contrato de publicación.

Los protocolos de respuesta anteriores no se convierten: exportar de nuevo con las
instrucciones vigentes. Los snapshots que incluyan lentes retiradas se rechazan.
