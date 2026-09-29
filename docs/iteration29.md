# Iteración 29 — Taxonomy Studio e intercambio LLM

- Navegación: Insights, Ingesta, Taxonomy Studio, Datos. Al abrir: Insights si hay
  NPS o Helix en el contexto; Ingesta si está vacío.
- La lente activa determina la única taxonomía exportada a Helix. Se elimina la
  selección por casillas. Cambiar de lente conserva sus resultados independientes.
- Cada catálogo se explora en su propio desplegable, con carga bajo demanda y
  paginación; la comparación tiene su propio estado.
- Manual ofrece Sin plantilla, Original y Descubierto según disponibilidad.
  Permite crear repetidamente o editar una existente. Cada guardado tiene un hash
  nuevo. Antes de sustituirla, muestra las relaciones afectadas y exige confirmación;
  la API rechaza confirmaciones ausentes y borradores con una revisión antigua.
  Las relaciones de comentarios se recalculan desde la plantilla o correspondencias
  editadas; Helix debe clasificarse de nuevo solo para Manual.
- Clasifica comentarios muestra procesados / total y pendientes del corpus, no el
  número de lotes de un ZIP aislado. Las importaciones parciales se acumulan y se
  pueden utilizar inmediatamente. Los siguientes ZIP excluyen las filas procesadas.
- Helix recibe una clasificación plana por incidencia, valida todo antes de escribir
  y presenta KPIs y distribución. Las anotaciones ajenas al contrato no se persisten.
  Los errores de estructura indican fichero y campo, sin volcar datos ni centenares
  de errores de validación.

## Comprobación con los adjuntos

Sobre una copia aislada de los datos locales y sin modificar los archivos originales:

- Designer: 9 Palancas y 29 Subpalancas.
- Dos respuestas Classifier: 400 de 16.845 comentarios procesados;
  siguiente exportación con 16.445 pendientes.
- Helix: 4.321 procesadas, 3.515 con categoría, 806 sin encaje,
  cobertura 81,35%, 605 vínculos NPS y ninguna pendiente.

Las instrucciones se mantienen únicamente en `taxonomy_prompts.py`; copiar las
nuevas instrucciones a los tres proyectos antes de procesar futuras exportaciones.
La WebApp estática no cambia. Las rutas de edición e intercambio siguen siendo
exclusivas de la aplicación local.
