# Instrucciones de proyectos e intercambio ZIP

Las plantillas oficiales están en `src/nps_lens/services/taxonomy_prompts.py` y
Taxonomy Studio permite copiarlas debajo de las URLs. Deben sustituir por completo
las instrucciones anteriores de **Crear Taxonomía**, **Clasifica comentarios** y **Clasifica incidencias**.
**Unifica conceptos** se configura exclusivamente en Configuración → Unificar conceptos,
heredando la compañía seleccionada.

## Flujo vigente

1. En Análisis estático se elige Original o Manual. Las equivalencias se mantienen
   por compañía en Configuración y se aplican a ambas taxonomías.
2. En Análisis con LLM se elige la lente Original, Manual o Descubierta. Los
   proyectos de comentarios e incidencias comparten esa lente y conservan sus
   resultados por separado.
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

## Contrato

Cada ZIP de entrada contiene un manifiesto versionado, las instrucciones, y lotes
deterministas de hasta 200 filas y 80.000 bytes. El manifiesto liga el intercambio
al corpus, taxonomía, conteos y SHA-256 de cada lote. Los IDs enviados son opacos,
secuenciales y no exponen las claves internas de negocio.

- Designer devuelve exactamente `manifest.json` y `taxonomy.json`.
- Classifier devuelve `manifest.json` y uno o varios
  `results/NNNNNN.json` completos.
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
El corpus exportable se limita a 64 MiB sin truncarlo; si lo supera, debe dividirse
explícitamente. Estas son salvaguardas locales, no límites garantizados de ChatGPT.

Las pruebas automatizadas verifican el contrato, seguridad, reanudación, reinicio,
idempotencia, atomicidad, orden y recorrido UI/API. La precisión semántica real del
modelo debe evaluarse aparte con un corpus etiquetado; una prueba simulada no puede
garantizarla.

## Helix y lente activa

Helix exporta únicamente la lente activa. Su ZIP de respuesta contiene el manifiesto
original y `results/NNNNNN.json`, con filas planas `id`, `lever`, `sublever`,
`secondary_classifications`, `rationale` y `links`. Los campos de anotación adicionales se ignoran y no se guardan.
Se validan IDs, orden, catálogo y cada vínculo antes de escribir ninguna asignación.
Los resultados se conservan por lente y huella del corpus. Recrear Manual cambia su
revisión e invalida sus resultados Helix, aunque las etiquetas sean iguales.

El estado distingue procesadas, pendientes, con categoría, sin encaje y vínculos NPS;
la cobertura es incidencias con categoría / total. Una incidencia sin encaje lleva
ambas etiquetas vacías y ningún vínculo, y no vuelve a exportarse como pendiente.

En causalidad LLM, la afinidad procede de los vínculos importados. El umbral local
no se aplica y su control queda desactivado. La ventana temporal sigue siendo
configurable y se comprueba en cada pareja comentario–incidencia.

Las instrucciones requieren interpretar el contexto de banca de empresas,
negaciones, tarea y resultado; no clasificar solo por palabras coincidentes. Una
valoración general comprensible no debe caer en «Información insuficiente».
Las categorías adicionales aportan contexto sin multiplicar los volúmenes NPS.
