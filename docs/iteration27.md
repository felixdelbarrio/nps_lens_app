> Documento histórico. El flujo vigente se describe en [iteración 28](iteration28.md).

# Iteración 27: taxonomías y causalidad Helix

En la aplicación local, Taxonomy Studio permite:

- **Origen**: conservar las Palancas/Subpalancas del fichero.
- **Completada**: crear, renombrar y eliminar categorías. El borrador parte de Origen;
  si no hay categorías de origen, parte de la taxonomía descubierta. Los cambios de
  nombre conservan las asignaciones; eliminar una categoría deja sus filas sin
  clasificar. Crear una categoría no asigna automáticamente comentarios.
- **Descubierta por LLM**: importar la respuesta ZIP de Designer o su `taxonomy.json`.
  La clasificación se importa por lotes completos. Las asignaciones solo se publican
  cuando se valida todo el intercambio.
- **Origen + Normalizada / Completada + Normalizada**: aplicar los sinónimos del
  registro de equivalencias. La taxonomía descubierta conserva las etiquetas del LLM.

## Comentarios pendientes

«Exportar comentarios pendientes» reutiliza la última taxonomía importada. Conserva
las asignaciones de comentarios cuyo ID y contenido siguen vigentes; envía los nuevos,
modificados o no clasificados. El ZIP de respuesta se valida contra el corpus exportado
antes de combinarlo con las asignaciones anteriores. Un cambio de taxonomía requiere
volver a clasificar el corpus.

## Helix Classifier

El enlace del proyecto es `https://chatgpt.com/g/g-p-6aba1edf109c81a4880f5420a0105b37`.
Las instrucciones de «Copiar» y «Ver» son las mismas incluidas en el ZIP.

1. Seleccionar la taxonomía y el método causal: Palanca, Subpalanca, Source Service N2,
   Journey roto o Journey de detracción.
2. Exportar a Descargas. Se incluyen incidencias pendientes, taxonomía y comentarios
   de contexto; no se envía nada automáticamente al proyecto.
3. Importar el ZIP de respuesta. El manifiesto debe conservar el tipo de taxonomía,
   método, identificador de intercambio y firmas. Se rechazan IDs ajenos, categorías
   inventadas, vínculos incoherentes y lotes incompletos.
4. Activar «Usar causalidad mediante LLM». Solo se habilita con todas las incidencias
   clasificadas para el contexto vigente. Al cambiar corpus, taxonomía o método se
   exige una respuesta válida para esa combinación. Es posible volver a reglas.

El modo LLM utiliza las asociaciones importadas, sin ejecutar el clasificador semántico
por reglas. Se mantienen filtros de periodo, ámbito y población, agregados y visualización.
La confianza del LLM expresa coincidencia semántica; no demuestra causalidad.

Los ZIP se leen en memoria con límites de tamaño, sin extraer ni ejecutar archivos.
El historial de trabajos y las clasificaciones conservadas tienen límites. Los controles
de intercambio, edición manual y activación LLM solo están disponibles en la aplicación
local; no se modifica el código de la WebApp estática.

La frase final del punto 5 del PDF está incompleta. Se ha aplicado su condición explícita:
no utilizar causalidad LLM sin importar y validar primero la respuesta Helix.
