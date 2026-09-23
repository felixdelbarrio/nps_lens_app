# NPS Lens WebApp

WebApp estática en Google Apps Script. Publica el snapshot generado por la aplicación local y conserva la misma navegación, cifras, racionales y evidencias en las vistas públicas. Los filtros NPS quedan fijados al contexto publicado; los criterios causales permanecen explícitos e independientes.

1. Crea una hoja de cálculo dentro del dominio BBVA y copia esta carpeta en un proyecto Apps Script.
2. Con la cuenta administradora inicial, selecciona y ejecuta `setupNpsLensWebApp` sin parámetros desde el editor. La instalación reutiliza la hoja configurada o crea **NPS Lens · Administración**, registra al ejecutor autorizado y muestra sus enlaces en el registro de ejecución. También admite parámetros explícitos para instalaciones avanzadas.
3. Despliega como WebApp para usuarios del dominio.
4. Entra como administrador y utiliza **Importar** para validar el ZIP generado en **Ingesta → Publicación Web** de la aplicación local. La WebApp exige las variantes completa y sin Evolución NPS, valida el límite de 30 MB y genera ambas presentaciones nativas de Google Slides antes de activar la edición.
   La transferencia del ZIP se realiza en bloques pequeños desde el navegador y se recompone temporalmente en Drive antes de validarlo. Esto evita el canal especial de subida de formularios de `google.script.run`, que puede fallar antes de ejecutar la función de servidor con el mensaje genérico `Form upload failed. Please try again.`.

El HTML inicial solo contiene el contexto de acceso. El shell analítico y las tablas de datos se guardan comprimidos, se recuperan bajo demanda y se cachean por edición. La WebApp no recalcula dashboards ni causalidad; `snapshot_load` mide el tiempo completo de recuperación y descompresión en el navegador.

Las acciones de **Newsletter**, **Importar** y **Configuración** solo se muestran al administrador. Newsletter mantiene una audiencia independiente por Owner Support Company, con destinatarios activos e inactivos, y permite un envío de prueba al administrador conectado. Configuración reúne la descarga de telemetría y el análisis de adopción sobre un único registro de actividad por lotes, sin almacenar filtros ni datos de cliente.

El snapshot Web fija el canal `Web`. El enlace de presentación de la newsletter es estable y resuelve en cada acceso la variante definida por el control global **Evolución NPS**.

El contrato de publicación `5.0` identifica cada edición exclusivamente por Owner Support Company, periodo y método causal. Incluye las pantallas ya calculadas y una sola página
acotada de NPS y Helix para las vistas de datos. Las taxonomías, modelos y corpus de
restauración permanecen en el snapshot taxonómico local independiente y no se incluyen
en el ZIP de la WebApp. El generador limita además `publication.json` a 20 MB antes de
crear el archivo, por debajo del límite de contenido descomprimido de Apps Script.

En local: `make WebApp` abre la última edición disponible en Descargas en `http://127.0.0.1:8625`.

### Alineación con la iteración 26

Las nuevas publicaciones incluyen el diagnóstico de población en **Causalidad → Situación del periodo**, también si el cruce no está disponible. Las cifras, coberturas, N1/N2 y motivos de exclusión se leen de `screens.linking.diagnostics`; la WebApp no recalcula matches ni porcentajes. Las ediciones antiguas sin este campo siguen mostrándose sin inventar valores.

Las páginas de Datos priorizan la identidad y el estado `match_status` de NPS, así como las fechas de ocurrencia/registro, el campo de origen de la fecha, la hoja y la elegibilidad de Helix, dentro del límite existente de 14 columnas y 500 filas. Las respuestas `non_matchable` no desaparecen del NPS global; la página conserva los filtros publicados.

El método publicado sigue siendo inmutable: las nuevas ediciones usan la preferencia local (por defecto `broken_journeys`), y `executive_journeys` continúa disponible cuando se publica expresamente. No se reclasifican las ediciones existentes.

Para aplicar los cambios en Google Apps Script, actualizar `App.html` y volver a desplegar. Para incorporar los datos corregidos y su trazabilidad, generar e importar un ZIP nuevo desde la aplicación local; actualizar el HTML no reprocesa snapshots antiguos. El contrato aditivo sigue siendo `5.0`.
