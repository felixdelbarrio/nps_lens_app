# Clasificación y vinculación Helix–VoC

El intercambio de «Clasifica incidencias» asigna categorías y evalúa los candidatos VoC con un único contrato e instrucciones. Las exportaciones pendientes incluyen automáticamente las incidencias con categorías cuya evaluación ya no está vigente; sus categorías se conservan exactamente. Una reevaluación explícita permite actualizar los vínculos ya evaluados, incluso si la evaluación previa no encontró coincidencias.

Se elimina el contenedor de vinculación de Taxonomy Studio. El método TF-IDF / reglas ↔ LLM se elige mediante un toggle en los filtros de Insights > Evidencia Helix–VoC. El backend calcula `linking_ready` para la lente, horizonte y canal seleccionados y exige comentarios clasificados, categorías de incidencias y evaluaciones vigentes. La API también rechaza activar LLM cuando falta un requisito. La clasificación de incidencias y la disponibilidad de vínculos mantienen contadores distintos.

El renderizado de gráficos se ejecuta en un proceso aislado con un máximo de 15 segundos. Ante bloqueo se termina el grupo de procesos y se usa el renderizador Pillow existente. No se vuelve a intentar un motor que ha fallado durante la sesión. Ambos renderizadores comparten tema y caché acotada. Se eliminan el parche de rutas y los reintentos anteriores. Los mensajes de éxito y fallo de las exportaciones ya no se borran al terminar la generación; un fallo al abrir Finder no invalida un archivo guardado.

El hook de empaquetado de python-pptx conserva el directorio `pptx/oxml`, necesario para resolver las plantillas XML de las notas desde el ejecutable macOS. Sin ese directorio, la ruta `oxml/../templates/notes.xml` fallaba aunque la plantilla estuviera incluida.

## Comprobación con datos locales

Se utiliza una copia SQLite y una copia de la configuración y Helix; las clasificaciones originales no se modifican. Las copias de datos y configuración se eliminan tras verificar, conservando los informes y la captura en `build/verification-20261007`.

- Argentina, agosto de 2026: toggle habilitado en el canal WEB con 1.902/1.902 incidencias procesadas, lente DISCOVERED, 93 vínculos y 85 incidencias con coincidencias. Interfaz sin errores de consola.
- Presentación completa: 20 diapositivas, 2.440.311 bytes, 34,31 segundos.
- Publicación Web: 4.594.731 bytes, 30,09 segundos; incluye ambas presentaciones.
- Ejecutable macOS: estado de vinculación disponible y presentación de 2.440.319 bytes en 20,75 segundos. Firma verificada con `codesign --verify --deep --strict`.
- Pruebas de componentes y API cubren posición del toggle, disponibilidad, persistencia del motor, evaluaciones que quedan pendientes al cambiar el horizonte/comentarios, conservación de categorías, reevaluación, limpieza del proceso de gráficos y mensajes de exportación.
- Prueba completa de navegador: pasa en 2,8 minutos, incluyendo creación de taxonomía, clasificación por lotes y comprobación del toggle en los filtros de Evidencia.

La aplicación macOS verificada se entrega en `build/verified-package/dist/nps-lens.app`. Se comprueba utilizando una copia de datos y configuración.
