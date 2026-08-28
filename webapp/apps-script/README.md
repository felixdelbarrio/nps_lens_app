# NPS Lens WebApp

WebApp estática en Google Apps Script. Publica el último JSON generado por la aplicación local y conserva la navegación analítica sin filtros públicos.

1. Crea una hoja de cálculo dentro del dominio BBVA y copia esta carpeta en un proyecto Apps Script.
2. Con la cuenta administradora inicial, selecciona y ejecuta `setupNpsLensWebApp` sin parámetros desde el editor. La instalación reutiliza la hoja configurada o crea **NPS Lens · Administración**, registra al ejecutor autorizado y muestra sus enlaces en el registro de ejecución. También admite parámetros explícitos para instalaciones avanzadas.
3. Despliega como WebApp para usuarios del dominio.
4. Entra como administrador y utiliza **Importar** para validar el ZIP generado en **Ingesta → Publicación Web** de la aplicación local. La WebApp valida el contrato y el límite de 30 MB, conserva el PPTX y genera la presentación nativa de Google Slides antes de activar la edición.

Las acciones de **Newsletter**, **Importar** y **Configuración** solo se muestran al administrador. Newsletter mantiene destinatarios activos e inactivos y permite un envío de prueba al administrador conectado. Configuración reúne la descarga de telemetría y el análisis de adopción sobre un único registro de actividad por lotes, sin almacenar filtros ni datos de cliente.

En local: `make WebApp` abre la última edición disponible en Descargas en `http://127.0.0.1:8625`.
