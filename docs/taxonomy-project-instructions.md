# Instrucciones de proyectos e intercambio ZIP

Las plantillas oficiales están en `src/nps_lens/services/taxonomy_prompts.py` y
Taxonomy Studio permite copiarlas debajo de las URLs. Deben sustituir por completo
las instrucciones anteriores de **Crear Taxonomía**, **Clasifica comentarios** y **Clasifica incidencias**.
**Crear similitud semántica** se configura en Análisis estático, debajo de Manual y
antes de Comparar taxonomías. **Unifica conceptos** se configura exclusivamente en Configuración → Unificar conceptos,
heredando la compañía seleccionada.

## Flujo vigente

1. El selector **Marco de clasificación**, antes de las pestañas, elige Original,
   Manual o Descubierta para toda la aplicación: Insights, comentarios, incidencias,
   presentaciones y snapshots. Solo muestra catálogos disponibles; sin ninguno,
   se deshabilitan el selector y los intercambios de clasificación, pero se puede
   crear una taxonomía manual o descubrirla con LLM.
2. La selección se guarda por Owner Support Company en
   `NPS_LENS_CLASSIFICATION_FRAMEWORKS` del `.env`. Cada taxonomía conserva sus
   resultados LLM al alternar marcos. Los nuevos comentarios/incidencias quedan
   pendientes; no invalidan los registros ya procesados. Cambiar el catálogo o la
   revisión manual exige reclasificar esa taxonomía. Un cambio en evidencia NPS
   enlazada invalida solo las incidencias que dependían de ella. Actualizar estado,
   fechas, responsable o enrutamiento operativo de una incidencia Helix conserva su
   categoría; solo un cambio de narrativa o taxonomía exige reclasificarla. En NPS,
   los cambios de metadatos conservan la categoría mientras el ID y Comment no cambien.
3. Crear Taxonomía exporta los comentarios; su respuesta contiene `manifest.json`
   y `taxonomy.json`. Su importación habilita el catálogo Descubierta para elegirlo.
4. Clasifica comentarios exporta solo pendientes de la lente elegida. Cada lote
   válido actualiza el progreso acumulado. Clasifica incidencias hace lo mismo
   con Helix, incluyendo evidencias NPS.
5. Comentarios y Causalidad permiten activar LLM cuando el ámbito visible está
   completamente procesado. Si cambian los filtros y falta alguna respuesta,
   utilizan automáticamente el análisis estático.
6. Unifica conceptos exporta comentarios, vocabulario y equivalencias actuales;
   importa `manifest.json` y `equivalences.json`. Valida compañía, corpus, alias y
   conflictos antes de actualizar los conceptos de esa compañía.

NPS Lens no abre ni controla Chrome, no almacena una sesión de ChatGPT y no pide
permisos de administración de aplicaciones en macOS. Los ZIP contienen comentarios
originales: solo deben subirse a un espacio corporativo autorizado.

## Criterios para Original y Manual

Crear similitud semántica utiliza el proyecto
https://chatgpt.com/g/g-p-6ac743802efc81a4a705074f9fd2644b y sus propias instrucciones
versionadas. Se elige Original o Manual sin cambiar el marco de clasificación activo.
La entrada incluye el corpus y `taxonomy.json.categories`; la respuesta contiene
exactamente `manifest.json` y `criteria.json` con `criteria` (ID → criterio) y `review`.
La importación valida corpus, catálogo, IDs completos, criterios y citas antes de
modificar el estado. No aplica los límites de tamaño del descubrimiento ni permite
renombrar, añadir o eliminar categorías.

Los criterios quedan asociados al fingerprint del catálogo base. Comentarios e
incidencias comparten el nuevo fingerprint semántico y sus pendientes se recalculan.
Las clasificaciones anteriores permanecen almacenadas bajo su fingerprint; cambiar
entre reglas y LLM no las elimina. Crear una Manual con exactamente las mismas
categorías conserva los criterios y copia las clasificaciones de comentarios LLM
válidas. Si se modifica el catálogo, conserva el historial y exige reclasificación.
Los snapshots congelan también los criterios. El Excel incluye los criterios usados
por la clasificación y se guarda en la carpeta configurada en la app local.

## Contrato

Cada ZIP de entrada contiene un manifiesto versionado, las instrucciones, y lotes
deterministas. Designer y normalizer conservan 200 filas / 80.000 bytes.
Classifier (`nps-lens-comments/3`) y Helix (`nps-lens-helix/3`) usan hasta 1.000
elementos / 300.000 bytes UTF-8 por lote, sin truncar elementos excesivos.
El manifiesto liga el intercambio
al corpus, taxonomía, conteos y SHA-256 de cada lote. Los IDs enviados son opacos,
secuenciales y no exponen las claves internas de negocio.

- Designer devuelve exactamente `manifest.json` y `taxonomy.json`.
- Classifier exporta todos los representantes pendientes de textos exactamente iguales,
  sin normalizar mayúsculas ni espacios. Antes resuelve localmente los vacíos
  (`fillna("")`) solo si existe `Sin clasificación temática / Información insuficiente`.
  Persiste cada asignación con la huella individual del comentario, incluidas las
  asignaciones propagadas a duplicados. Si todo queda resuelto, no crea un ZIP.
- Classifier devuelve `manifest.json` y uno o varios `results/NNNNNN.json` completos:
  `{"classifications":[{"id":"1","primary":"c001","secondary":["c002"]}]}`.
  `taxonomy.json.categories` contiene el mapping determinista ID → lever/sublever;
  se reconstruyen las etiquetas antes de persistirlas. No cambia analytics/reporting.
- Una descarga prepara todos los ZIP pendientes, con un lote por archivo, en una
  carpeta propia de Descargas: `1_2_comentarios.zip`, `2_2_comentarios.zip` o
  `1_2_incidencias_helix.zip`, `2_2_incidencias_helix.zip`. La UI muestra la carpeta
  y los archivos. Cada respuesta se importa por separado, en cualquier orden.
  Importar actualiza el progreso sin generar otros ZIP. Una nueva descarga manual
  contiene solo pendientes. Se conservan las tres últimas series completas.
- ZIP classifier/Helix anteriores a v3 se rechazan explícitamente. Las asignaciones
  ya persistidas siguen vigentes mientras sus huellas y catálogos sean válidos.
- La clasificación parcial es reanudable incluso tras reiniciar NPS Lens, y sus
  asignaciones se pueden explorar y utilizar inmediatamente.
- Repetir una respuesta idéntica es idempotente; una respuesta diferente para un
  lote ya importado se rechaza.
- Un cambio del corpus, IDs ausentes o reordenados, categorías inventadas, JSON con
  campos extra o un manifiesto alterado invalidan la importación completa.

La taxonomía admite como máximo diez Palancas y cuatro Subpalancas por Palanca e
incluye siempre `Sin clasificación temática / Información insuficiente` y
`Sin clasificación temática / Tema no cubierto`. Estos límites se aplican al diseñador; Original y Manual conservan su catálogo.
El clasificador devuelve una pareja principal y hasta dos parejas adicionales
cuando existen temas independientes explícitos. Se conserva el orden del lote;
se rechazan parejas desconocidas o repetidas. Los cálculos NPS utilizan la pareja
principal y cada respuesta se cuenta una sola vez.

## Seguridad y límites

Los archivos se leen directamente desde el ZIP; nunca se extraen ni ejecutan.
Se rechazan rutas absolutas o ascendentes, enlaces, entradas cifradas, métodos de
compresión desconocidos, nombres duplicados, JSON no UTF-8, claves duplicadas,
NaN/Infinity y archivos o expansiones por encima de los límites. La escritura en
Descargas es atómica.

El límite es 32 MiB comprimidos, 128 MiB expandidos, 2 MiB por JSON y 4.096 entradas.
Designer conserva el límite de corpus de 64 MiB. Classifier y Helix dividen todos los pendientes en ZIP de un lote cada uno.
Helix sigue incluyendo toda la evidencia NPS y rechaza corpus que superen la seguridad ZIP. Estas son salvaguardas locales, no límites garantizados de ChatGPT.

Las pruebas automatizadas verifican el contrato, seguridad, reanudación, reinicio,
idempotencia, atomicidad, orden y recorrido UI/API. La precisión semántica real del
modelo debe evaluarse aparte con un corpus etiquetado; una prueba simulada no puede
garantizarla.

## Helix y lente activa

Helix exporta únicamente la lente activa. Su ZIP de respuesta contiene el manifiesto
original y `results/NNNNNN.json`, con filas planas `id`, `primary`, `secondary`,
`rationale` y `links`. `taxonomies.json` contiene el mapping de categorías por lente.
Las evidencias NPS usan los mismos IDs en `primary` y `secondary`, conservando todo
el corpus y los IDs enlazables. Cada ZIP envía un lote de hasta 1.000 incidencias pendientes;
no se deduplican por descripción ni se preclasifican las vacías. Los campos de anotación adicionales se ignoran y no se guardan.
Se validan IDs, orden, catálogo y cada vínculo antes de escribir ninguna asignación.
Los resultados se conservan por lente y huella del corpus. Recrear Manual cambia su
revisión e invalida sus resultados Helix, aunque las etiquetas sean iguales.

El estado distingue procesadas, pendientes, con categoría, sin encaje y vínculos NPS;
la cobertura es incidencias con categoría / total. «Con más de una categoría»
cuenta registros con una principal y al menos una adicional, una sola vez por
registro; las adicionales no multiplican los totales ni el NPS. Una incidencia sin encaje lleva
`primary=null`, `secondary=[]` y ningún vínculo, y no vuelve a exportarse como pendiente.

Taxonomy Studio ordena Helix en dos pasos: clasificar incidencias y vincular Helix ↔ VoC.
La clasificación completa de la lente activa habilita el interruptor de vinculación
LLM; sólo al seleccionarlo aparece el intercambio de vínculos que reutiliza las
categorías. Evidencia consume el motor seleccionado. Las categorías recibidas y
los vínculos pendientes se contabilizan por separado.

En vinculación LLM, la afinidad procede de los vínculos importados. El umbral local
no se aplica y su control queda desactivado. La ventana temporal sigue siendo
configurable y se comprueba en cada pareja comentario–incidencia: sólo admite
incidencias de la misma fecha o hasta N días antes del comentario. El diagnóstico
canónico mide p50, p90, p95, máximo y comentarios con más de 1, 5 y 10 incidencias
únicas entre comentarios enlazados, sin limitar incidencias por comentario.

Las instrucciones requieren interpretar el contexto de banca de empresas,
negaciones, tarea y resultado; no clasificar solo por palabras coincidentes. Una
valoración general comprensible no debe caer en «Información insuficiente».
Las categorías adicionales aportan contexto sin multiplicar los volúmenes NPS.

Python puede usarse en ChatGPT para leer, indexar, validar y construir ZIP/JSON,
nunca para ejecutar datos de entrada ni sustituir decisiones semánticas por reglas.
Cada ZIP se procesa por separado y la respuesta conserva su número de archivo.
Los límites reales de la sesión del modelo pueden impedir completar un lote;
la división no garantiza su capacidad de procesamiento. Hay que actualizar las instrucciones de ambos Proyectos ChatGPT.
