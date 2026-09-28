# Taxonomy Engine local

La ingesta conserva `source_channel`, `source_lever`, `source_sublever` y el comentario
original. La identidad usa el ID de origen y el contexto; sin ID, fecha, nota, comentario,
usuario y contexto. Cambiar categorías o equivalencias no crea una respuesta nueva.

`TaxonomyResolver` es la frontera única que entrega Canal/Palanca/Subpalanca a las
analíticas existentes, incluidos el vínculo NPS–Helix, informes y publicación.

## Lentes

- `SOURCE` — Original: categorías del fichero con equivalencias explícitas al resolver.
- `COMPLETED` — Manual: catálogo editable, asignaciones conservadas al renombrar y
  equivalencias aplicadas al resolver. No entrena ni clasifica mediante ML local.
- `DISCOVERED` — Descubierta por LLM: categorías y clasificaciones importadas de los
  dos proyectos GPT, sin aplicar equivalencias a sus Palancas/Subpalancas.

Palanca y Subpalanca son opcionales. Studio informa cobertura y estado
COMPLETE/PARTIAL/MISSING/NO_TEXT; permite editar, explorar, comparar y activar lentes.
El protocolo y el flujo de usuario se detallan en [iteración 28](iteration28.md).

## Persistencia y reproducción

SQLite guarda configuración y asignaciones por identidad estable. La firma incluye
corpus, versión del motor y parámetros; Manual incluye también las etiquetas originales.
Las equivalencias se aplican durante la resolución y no invalidan las asignaciones.
Cambiar filtros no genera modelos ni inicia intercambios.

Configuración → Snapshots permite elegir lente publicada y política ACTIVE_ONLY,
SOURCE_AND_ACTIVE o ALL_AVAILABLE. El snapshot incluye registros, asignaciones,
equivalencias y configuración, con checksum. La restauración usa asignaciones guardadas,
sin modelos; mantiene un estado separado del corpus local. Exportación y restauración
tienen límite de 30 MB. Solo se aceptan las tres lentes vigentes.

La publicación conserva los campos analíticos que consume la WebApp. El snapshot
taxonómico se exporta y restaura por separado de la publicación estática.

## Histórico

Antes de migrar un esquema antiguo se crea una copia `.before-taxonomy.sqlite3`.
Se recuperan categorías originales desde archivos de carga retenidos y se consolidan
identidades antiguas que dependían de etiquetas mutables. Si esos archivos no existen,
Studio informa de originales no recuperables. Las actualizaciones conservan las
referencias de cargas a registros.

Las etiquetas y asociaciones importadas requieren revisión de negocio. El snapshot
preserva los datos; no convierte una asociación NPS–Helix en causalidad.
