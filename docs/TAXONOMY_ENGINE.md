# Taxonomy Engine local

La ingesta conserva `source_channel`, `source_lever`, `source_sublever` y el comentario
original. La identidad usa el ID de origen y el contexto; sin ID, fecha, nota, comentario,
usuario y contexto. Cambiar categorías o equivalencias no crea una respuesta nueva.

`TaxonomyResolver` es la frontera única que entrega Canal/Palanca/Subpalanca a las
analíticas existentes, incluidos el vínculo NPS–Helix, informes y publicación.

## Lentes

- SOURCE: categorías originales, incluidas las ausencias.
- NORMALIZED: equivalencias categóricas explícitas, separadas por dominio y dimensión.
- COMPLETED: completa huecos a partir de texto; TF-IDF disperso y regresión logística,
  evaluación Macro-F1 en holdout y umbral de asignación. No cambia etiquetas existentes.
- DISCOVERED: agrupación local del texto con K acotado, términos y ejemplos representativos.
  No utiliza notas NPS como características.

Palanca y Subpalanca son opcionales. El Studio informa cobertura y estado
COMPLETE/PARTIAL/MISSING/NO_TEXT; permite generar, explorar, comparar y activar lentes.
Los cálculos de generación solo se ejecutan mediante acciones explícitas.

## Persistencia y reproducción

SQLite guarda configuración y asignaciones por identidad estable. La firma de caché
incluye corpus, versión del motor y parámetros; COMPLETED también incluye etiquetas
y equivalencias NPS relevantes. Cambios de filtros no entrenan; artefactos caducados
requieren regeneración explícita. Las equivalencias Helix no invalidan modelos NPS.

Configuración → Snapshots permite elegir lente publicada y política ACTIVE_ONLY,
SOURCE_AND_ACTIVE o ALL_AVAILABLE. El snapshot incluye registros, asignaciones,
equivalencias y configuración, con checksum. La restauración usa asignaciones guardadas,
sin entrenar ni requerir el modelo serializado; mantiene un estado separado del corpus local.
Volver a datos locales recupera ese corpus. Exportación y restauración tienen límite de 30 MB.
La publicación conserva únicamente los campos analíticos que consume la WebApp. El
snapshot taxonómico se exporta y restaura por separado para no trasladar corpus ni
modelos a la publicación estática.

## Histórico

Antes de migrar un esquema antiguo se crea una copia `.before-taxonomy.sqlite3`.
Se recuperan categorías originales desde los archivos de carga retenidos y se consolidan
identidades antiguas que dependían de etiquetas mutables. Si esos archivos no existen,
no se pueden reconstruir valores ya sobrescritos: el Studio informa de originales no
recuperables. Las actualizaciones de cargas conservan sus referencias a los registros.

## Límites

Los modelos son locales y dependen de la cobertura y calidad del texto. La evaluación
puede impedir completar una categoría; esos huecos se conservan. Los temas descubiertos
son agrupaciones textuales, no una taxonomía de negocio validada por una persona.
El snapshot preserva etiquetas y datos; no convierte una asociación NPS–Helix en causalidad.
