# Instrucciones de proyectos e intercambio ZIP

Las plantillas oficiales están en `src/nps_lens/services/taxonomy_prompts.py` y
Taxonomy Studio permite copiarlas debajo de las URLs. Deben sustituir por completo
las instrucciones anteriores de **Crea Taxonomía** y **Clasifica taxonomía**.

## Flujo vigente

1. Taxonomy Studio exporta `nps-lens-designer-<job>.zip` a Descargas.
2. El usuario lo adjunta al proyecto Crea Taxonomía en su navegador habitual.
3. El proyecto devuelve un ZIP con `manifest.json` y `taxonomy.json`.
4. Al importarlo, NPS Lens valida la taxonomía y exporta
   `nps-lens-classifier-<job>.zip`.
5. El usuario lo adjunta a Clasifica taxonomía e importa su ZIP de respuesta.
6. NPS Lens publica DISCOVERED únicamente tras validar todos los lotes.

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
- La clasificación parcial es reanudable incluso tras reiniciar NPS Lens, pero no
  se hace visible como taxonomía.
- Repetir una respuesta idéntica es idempotente; una respuesta diferente para un
  lote ya importado se rechaza.
- Un cambio del corpus, IDs ausentes o reordenados, categorías inventadas, JSON con
  campos extra o un manifiesto alterado invalidan la importación completa.

La taxonomía admite como máximo diez Palancas y cuatro Subpalancas por Palanca e
incluye siempre `Sin clasificación temática / Información insuficiente` y
`Sin clasificación temática / Tema no cubierto`. El clasificador devuelve una
única pareja válida por comentario, en el mismo orden.

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
