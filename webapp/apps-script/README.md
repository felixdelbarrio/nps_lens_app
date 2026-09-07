# NPS Lens WebApp

WebApp estática en Google Apps Script. Publica el snapshot generado por la aplicación local y conserva la misma navegación, cifras, racionales y evidencias en las vistas públicas. Los filtros NPS quedan fijados al contexto publicado; los criterios causales permanecen explícitos e independientes.

1. Crea una hoja de cálculo dentro del dominio BBVA y copia esta carpeta en un proyecto Apps Script.
2. Con la cuenta administradora inicial, selecciona y ejecuta `setupNpsLensWebApp` sin parámetros desde el editor. La instalación reutiliza la hoja configurada o crea **NPS Lens · Administración**, registra al ejecutor autorizado y muestra sus enlaces en el registro de ejecución. También admite parámetros explícitos para instalaciones avanzadas.
3. Despliega como WebApp para usuarios del dominio.
4. Entra como administrador y utiliza **Importar** para validar el ZIP generado en **Ingesta → Publicación Web** de la aplicación local. La WebApp exige las variantes completa y sin Evolución NPS, valida el límite de 30 MB y genera ambas presentaciones nativas de Google Slides antes de activar la edición.

El HTML inicial solo contiene el contexto de acceso. El shell analítico y las tablas de datos se guardan comprimidos, se recuperan bajo demanda y se cachean por edición. La WebApp no recalcula dashboards ni causalidad; `snapshot_load` mide el tiempo completo de recuperación y descompresión en el navegador.

Las acciones de **Newsletter**, **Importar** y **Configuración** solo se muestran al administrador. Newsletter mantiene destinatarios activos e inactivos y permite un envío de prueba al administrador conectado. Configuración reúne la descarga de telemetría y el análisis de adopción sobre un único registro de actividad por lotes, sin almacenar filtros ni datos de cliente.

El snapshot Web fija el canal `Web`. El enlace de presentación de la newsletter es estable y resuelve en cada acceso la variante definida por el control global **Evolución NPS**.

En local: `make WebApp` abre la última edición disponible en Descargas en `http://127.0.0.1:8625`.
