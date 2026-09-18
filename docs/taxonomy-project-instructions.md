# Instrucciones de proyectos y contrato batch

Las plantillas oficiales de la aplicación están en
`src/nps_lens/services/taxonomy_prompts.py`. Taxonomy Studio permite copiarlas
debajo de cada URL y consultarlas sin iniciar Chrome. Si el portapapeles está
bloqueado, muestra el texto seleccionable. Copiar no actualiza el proyecto remoto:
pega la plantilla correspondiente en sus instrucciones y sustituye las anteriores.

## Por qué se han ajustado

Las instrucciones anteriores de los proyectos no coincidían con el adaptador:
`results` frente a `classifications`, `comment` frente a `Comment`,
Subpalancas con objetos frente a cadenas, y campos adicionales obligatorios
(`description`, `confidence`, `needs_review`, evidencia, métricas y versiones)
que el validador rechazaba. La aplicación tampoco tiene un circuito de revisión
humana basado en esos campos. No se simula esa capacidad.

Se conservan el criterio semántico, «menos es más», la distinción entre fricción
y producto/canal/journey, y la prohibición de inferir causalidad. Las etiquetas
deben ser autoexplicativas porque el contrato vigente no transporta definiciones.

## Contrato vigente

- **Crea Taxonomía:** corpus completo con IDs opacos y Comment; devuelve solamente
  `{"taxonomy":[{"lever":"...","sublevers":["..."]}]}`. Máximo 10 Palancas,
  incluida la de reserva, y 4 Subpalancas por Palanca; no se rellenan cuotas.
- **Clasifica taxonomía:** la misma taxonomía para todos los lotes; devuelve
  `{"classifications":[{"id":"...","primary_classification":{"lever":"...","sublever":"..."}}]}`.
  Una asignación por ID. El backend valida todos los IDs y relaciones y restituye
  el orden de entrada antes de publicar atómicamente.
- **Reserva:** `Sin clasificación temática` con `Información insuficiente`
  y `Tema no cubierto`. Son resultados explícitos, no confianza calibrada ni
  una aprobación humana. Tener una asignación no demuestra precisión semántica.

El batch incorpora las mismas instrucciones que se copian. El contenido de los
comentarios es dato no confiable y se serializa como JSON, separado de las reglas.
No se aceptan claves JSON duplicadas, NaN, Infinity ni campos ajenos al contrato.
Las categorías no pueden duplicarse por mayúsculas/Unicode ni tener varios padres.

## Recursos y límites

El clasificador llena lotes de manera determinista y lineal, respetando el máximo
de filas configurado y presupuestos de 100.000 caracteres de prompt y 40.000 de
respuesta compacta estimada con las etiquetas más largas. Son salvaguardas de
la aplicación, **no límites de tokens garantizados por ChatGPT**.

Designer realiza una única llamada si el corpus cabe. Para corpus mayores,
procesa particiones deterministas que incluyen todos los comentarios y consolida
sus propuestas en una taxonomía global. Si las propuestas tampoco caben juntas,
la consolidación se realiza por niveles, siempre reduciendo el número de
candidatas. Solo después se clasifica todo el corpus con la misma taxonomía.
No se muestrea, no se trunca y no se publica ninguna propuesta intermedia.
`INPUT_TOO_LARGE` se reserva para un comentario individual que no cabe en una
petición; se detecta antes de abrir el navegador. Los límites son por petición,
no por corpus. La calidad semántica de la consolidación necesita evaluación real.

El estado de sesión consulta el contexto existente sin abrir ni navegar Chrome.
Durante el login la UI lo consulta cada cinco segundos mientras está visible.
«Sesión iniciada» indica autenticación detectada, no acceso ya verificado a los
proyectos. «Descubrir» verifica ese acceso automáticamente en el mismo contexto.

La versión de instrucciones se calcula a partir de las plantillas y límites y
forma parte de la firma de caché. Cambiarlas invalida resultados locales de
descubrimiento; no altera SOURCE, NORMALIZED, COMPLETED ni snapshots históricos.

## Verificación

Las pruebas offline comprueban contrato, presupuestos, unicidad, orden,
atomicidad, copia exacta y fallos de portapapeles. No prueban la precisión de un
modelo real: esta requiere un corpus etiquetado de evaluación, especialmente
con comentarios ambiguos, multitema, multilingües y sin información suficiente.

La separación entre instrucciones y datos sigue la
[guía oficial de prompting](https://developers.openai.com/api/docs/guides/prompt-engineering).
En la interfaz web, pedir JSON no equivale a disponer de Structured Outputs de
la API: la validación local sigue siendo obligatoria.
