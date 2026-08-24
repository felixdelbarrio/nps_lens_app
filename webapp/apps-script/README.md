# NPS Lens WebApp

WebApp estática en Google Apps Script. Publica el último JSON generado por la aplicación local y conserva la navegación analítica sin filtros públicos.

1. Crea una hoja de cálculo dentro del dominio BBVA y copia esta carpeta en un proyecto Apps Script.
2. Ejecuta `setupNpsLensWebApp(spreadsheetId, "administrador@bbva.com")`.
3. Sube `publication.json` y el PPTX a Drive y ejecuta `setPublishedEditionFiles({editionFileId, reportFileId})`.
4. Despliega como WebApp para usuarios del dominio.

En local: `make WebApp` abre la última edición disponible en Descargas en `http://127.0.0.1:8625`.
