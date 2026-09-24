# Análisis de telemetría del 23 de septiembre de 2026

Fuente: `nps-lens-telemetria-20260923-163958.json`, exportado a las 16:39:58 UTC.

## Evidencia y límites

97 peticiones: 95 respuestas HTTP 200 y dos HTTP 500, ambas `MergeError` en
`GET /api/dashboard/linking`. El JSON no contiene trazas ni los datos usados:
no permite identificar qué cruce falló en aquella ejecución. La base de datos
configurada actualmente tampoco está disponible para repetir esa sesión.

| Operación | Evidencia de la sesión | Corrección aplicada |
| --- | --- | --- |
| Vinculación NPS–Helix | Dos errores tras 25,35 y 22,62 s; consultas correctas de 35,02–39,17 s | Identidad interna compartida en matching, referencias de atribución, hotspots y evidencia. Evita colisiones entre respuestas distintas con el mismo ID externo cuando existe `_business_key`. Se conservan las validaciones de cardinalidad: no se ocultan conflictos descartando respuestas. |
| Guardar preferencias | Hasta 35,29 s, finalizando junto a la vinculación | Guardar preferencias ya no espera al bloqueo analítico ni vacía las cachés. El método causal efectivo y la URL Helix forman parte de la clave de caché; los filtros ya estaban incluidos. Los cambios en datos, taxonomías, equivalencias y jerarquía siguen invalidando resultados. |
| Taxonomy Studio | Crecimiento de 1,26 a 7,41 s durante las cargas | Comparte una lectura del corpus con dashboard y equivalencias; calcula la disponibilidad de taxonomías una sola vez por solicitud. Los snapshots reutilizan esa misma disponibilidad. |
| Importación NPS | Nueve cargas de 1,69–11,05 s | Las importaciones NPS y Helix ejecutan lectura y procesamiento síncronos en el pool de trabajadores de FastAPI, liberando el bucle de eventos para atender otras peticiones. Esto mejora la capacidad de respuesta, no elimina el coste de leer Excel. |
| Dashboard NPS / equivalencias | Hasta 9,35 s / 4,24 s | Reutilización del corpus leído y conservación de cachés al guardar preferencias. |
| PowerPoint | Una exportación de 17,08 s | La caché causal existente sobrevive a los guardados de preferencias y puede reutilizarse. No se ha medido una reducción del tiempo de renderizado del informe. |

Los conflictos residuales de cardinalidad devuelven HTTP 409 con una indicación
para revisar duplicados y corregir la carga. No se generan resultados ambiguos
ni se exponen mensajes internos con posibles identificadores. La telemetría
conserva el tipo `MergeError` para el diagnóstico.

Las 13 peticiones agrupadas como `<unmatched>` terminaron con HTTP 200.
Esa etiqueta evita guardar rutas no reconocidas; por sí sola no indica un error
y puede corresponder a recursos estáticos.

## Memoria y observabilidad

`rss_mb` utiliza `ru_maxrss`: es el **máximo histórico** de memoria residente del
proceso, no la memoria actual de cada petición. Los 1329,359 MB registrados no
prueban una fuga. `cpu_ms` mide el incremento de CPU del proceso e incluye el
trabajo de peticiones concurrentes. El JSON nuevo declara ambas semánticas sin
cambiar los nombres existentes ni incorporar datos personales.

La nueva caché de corpus conserva únicamente un propietario/revisión, devuelve
copias independientes y se libera con la invalidación explícita. Detecta cambios
en SQLite y su fichero WAL; no conserva un corpus por cada importación. Las
cachés analíticas existentes siguen teniendo límites de entradas. Retener un
corpus supone memoria adicional a cambio de evitar lecturas repetidas; no se
ha medido el pico con los datos de la sesión original.

## Validación

Regresiones específicas para:

- Matching → atribución con IDs externos repetidos y claves internas distintas,
  manteniendo ambas respuestas, sus comentarios y puntuaciones.
- Guardar preferencias mientras otro hilo mantiene el bloqueo analítico, sin
  descartar resultados en caché.
- Cambiar el método causal por defecto sin devolver una respuesta de la caché anterior.
- Lecturas concurrentes compartidas del corpus, aislamiento entre copias,
  cambio de propietario, invalidación y actualización del estado con SQLite WAL.
- Disponibilidad de taxonomías validada una sola vez por petición de Studio.
- Disponibilidad de `/api/health` durante importaciones NPS y Helix bloqueadas.
- Semántica explícita y conservación del contrato de privacidad de telemetría.

Para medir la mejora real, se debe repetir la secuencia con el mismo corpus y
filtros, incluyendo una segunda consulta idéntica y un guardado de preferencias
entre consultas. No se extrapolan porcentajes de mejora desde pruebas sintéticas.
