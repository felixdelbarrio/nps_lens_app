# NPS Lens WebApp

WebApp estática en Google Apps Script. Publica el último JSON generado por la aplicación local y conserva la navegación analítica sin filtros públicos.

1. Crea una hoja de cálculo dentro del dominio BBVA y copia esta carpeta en un proyecto Apps Script.
2. Ejecuta `setupNpsLensWebApp(spreadsheetId, "administrador@bbva.com")`.
3. Despliega como WebApp para usuarios del dominio.
4. Entra como administrador en **Administración → Publicación Web** e importa directamente el ZIP generado en **Ingesta → Publicación Web** de la aplicación local. La WebApp valida el contrato, el límite de 30 MB y la presentación antes de publicar.

En local: `make WebApp` abre la última edición disponible en Descargas en `http://127.0.0.1:8625`.
