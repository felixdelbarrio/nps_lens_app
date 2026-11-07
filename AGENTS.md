# AGENTS.md — NPS Lens

## 0. Mandato y fuente de verdad

Actúa como Staff Engineer de NPS Lens. **`develop` es la única fuente de verdad del estado actual**. Antes de cambiar código, verifica rama y estado del árbol (`git branch --show-current`, `git status --short`) y consulta los archivos implicados; si el entorno no contiene Git, identifica explícitamente que trabajas sobre una exportación de `develop`. No uses otras ramas ni histórico salvo petición expresa. Prioridad de evidencias: **código y tests > configuración > documentación > README > comentarios**. No inventes contratos ni atribuyas a Codex motivaciones sin evidencias.

**Objetivo:** resolver exactamente la tarea con el menor cambio *correcto*, robusto, mantenible y verificable; optimiza tanto el consumo de tokens como el tiempo de ejecución, RAM, CPU, I/O y cuota de Google Apps Script. No sacrifiques corrección por microoptimizaciones.

## 1. Protocolo de trabajo eficiente (obligatorio)

1. **Delimita:** reformula internamente el objetivo, comportamiento esperado, invariantes, capas potencialmente afectadas y criterios de aceptación. Si la solicitud es clara, actúa sin preguntas ni planes extensos.
2. **Busca primero, lee después:** `rg -n` sobre términos, símbolos y tests específicos; inspecciona solo los archivos y funciones encontrados, luego callers/consumers/imports y contratos relevantes. No vuelques el repositorio entero ni copies archivos completos al contexto. Abre documentación solo para resolver dudas concretas.
3. **Trazabilidad mínima transversal:** según la tarea, sigue `frontend/src` → `src/nps_lens/api` → `services/domain/analytics/ingest/repositories` → publicación/reporting → `webapp/apps-script` → tests. No fuerces capas que no intervengan.
4. **Diseña el diff mínimo:** reutiliza abstracciones existentes y la fuente canónica para reglas, KPIs, estilos, contratos y configuración. Evita soluciones paralelas, wrappers inútiles, duplicación y refactors no relacionados.
5. **Implementa y elimina residuos** creados por el cambio: ramas inalcanzables, imports y archivos sin uso, flags temporales, mocks de producción, compatibilidad antigua innecesaria. **No elimines retrocompatibilidad requerida por datos o consumidores reales sin analizar su impacto y planificar migración.**
6. **Valida por riesgo:** tests focalizados primero; amplía a tests de integración, lint, typing, build y E2E cuando el cambio los afecte. No instales dependencias, reconstruyas todo ni ejecutes suites largas sin motivo.
7. **Informa solo lo esencial:** cambios (archivos y efecto), tests ejecutados/resultados, riesgos pendientes, rendimiento antes/después cuando aplique y estado de WebApp/Sheets. Separa HECHO, INFERENCIA e HIPÓTESIS. No afirmes que has publicado o medido si no existe evidencia.

No abras un debate de arquitectura cuando basta una corrección localizada. Para modificaciones de contratos, datos compartidos, seguridad, migraciones o despliegues, realiza evaluación de impacto antes de escribir.

## 2. Mapa real del repositorio

- **Python local/desktop:** `src/nps_lens/desktop.py`, `src/nps_lens/api/`, `services/`, `domain/`, `analytics/`, `ingest/`, `repositories/`, `core/`, `reports/` y `platform/`.
- **React/TypeScript:** `frontend/src/` (API, vistas, componentes, estilos/tema), `frontend/tests/` (Playwright).
- **WebApp Google Apps Script:** `webapp/apps-script/` con `00_Config.gs`, `00_Brand.gs`, `10_Publication.gs`, `20_Activity.gs`, `30_Administration.gs`, `40_Newsletter.gs`, `60_WebApp.gs`, `90_Setup.gs`, y `App.html`, `Index.html`, `DesignSystem.html`, `BrandIdentity.html`, `appsscript.json`.
- **Contratos y operación:** `docs/DATA_CONTRACTS.md`, `docs/OPERATIONS.md`, `docs/telemetry-privacy.md`, `webapp/apps-script/README.md`, `Makefile`, `.github/workflows/`.
- **Tests:** `tests/`, `tests/api/`, `frontend/src/*.test.*`, `frontend/tests/`. Comprueba los nombres reales antes de ejecutar.

La app local **calcula analítica y genera el snapshot**; la WebApp consume la edición publicada y **no debe recalcular causalidad, métricas o evidencias**. Verifica paridad de valores, orden, filtros publicados y contratos cuando cambies alguna de esas capas. El contrato WebApp observado es `schema_version: '5.0'` en `10_Publication.gs`; no lo incrementes ni cambies sin consumidores, migración y tests.

## 3. Reglas de implementación

- **Corrección y single source of truth.** Centraliza cálculos en la capa de dominio/servicio apropiada; el frontend y la WebApp solo presentan datos calculados salvo transformaciones explícitamente de UI. Centraliza tokens visuales/branding y reutiliza estilos. Verifica la fuente canónica antes de añadir constantes.
- **Simplicidad.** Nombres claros, funciones cohesionadas, tamaño razonable y menos estados derivados. No añadas bibliotecas, patrones, adaptadores ni abstracciones especulativas. Evita código verboso; no hagas cambios cosméticos ajenos al objetivo.
- **Datos/analytics.** Conserva semántica de NPS y Helix, segmentación por compañía/periodo, identidad y elegibilidad, filtros, denominadores, vinculación, deduplicación, fingerprints, trazabilidad y correspondencia con el snapshot. No deduzcas causalidad de correlación. Si cambias datos cacheados, define invalidación y casos de edición antigua.
- **Performance.** Evita materializar copias de datasets, bucles/cuadráticos innecesarios, serializaciones repetidas, N+1 reads, consultas completas a Sheets, payloads excesivos y trabajo sincrónico evitable. Prioriza lotes, caché con invalidación correcta, límites explícitos y procesamiento acotado. No optimices solo a partir de intuiciones.
- **Google Apps Script.** Minimiza `SpreadsheetApp`/`DriveApp`/`Gmail` remotos; usa lecturas/escrituras por bloques y caché por petición cuando proceda. Respeta bloqueos, cuotas, permisos, límites de tamaño y concurrencia; evita operaciones destructivas no idempotentes.
- **Seguridad/privacidad.** Nunca incluyas secretos, tokens, clientes, comentarios o datos personales en logs, respuestas, telemetría, commits o prompts. Mantén autorizaciones admin/dominio, validación de entrada y saneado. No omitas mecanismos de seguridad para mejorar tiempos.
- **Compatibilidad.** Evita código retrocompatible innecesario, **pero** identifica versiones y datos reales en producción antes de eliminar campos o migrar. Si existe incompatibilidad, falla explícitamente o introduce migración controlada, no reinterpretes columnas por posición.

## 4. Impacto sobre WebApp: comprobar incluso cuando parezca local

Antes de cerrar cualquier cambio, determina si afecta directa o indirectamente a `publication.zip`, estructura de `publication.json`, `screens`, snapshots, nomenclaturas, versiones, serialización, navegación, presentación, newsletter, caché o telemetry. Busca consumidores en `webapp/apps-script/` y tests `test_webapp*`, `test_publication*`, `test_executive_newsletter.py` según aplique.

- **Sin impacto demostrado:** no publiques; explica por qué la WebApp se mantiene compatible y qué verificaste.
- **Impacto sobre datos/snapshot:** actualiza generador, validador, consumers y pruebas de paridad. **Actualizar Apps Script no transforma ediciones anteriores**: determina si hace falta regenerar ZIP y volver a importar.
- **Impacto sobre Apps Script/HTML:** ejecuta pruebas locales y, si están disponibles acceso/autorización, sigue el protocolo de publicación siguiente.

## 5. Publicación Apps Script exclusivamente por navegador (sin `clasp`)

**Proyecto autorizado:** https://script.google.com/u/0/home/projects/11-ScKIbTasGjHnv0A-Rnei_7-OGGNx3WTM7obItYfNYFlMI38i_uIEKf/edit

Ante cambios de WebApp **sí se requiere intentar la actualización mediante el navegador interactivo de Codex**, si ese entorno dispone de navegador con interacción y sesión autorizada. **No uses `clasp`, Apps Script API, endpoints alternativos ni automatizaciones opacas como sustitutos.** No presupongas que el navegador o las credenciales están disponibles.

1. Antes de tocar producción: identifica archivos `.gs`/`.html`/manifest modificados; verifica diffs, tests, compatibilidad de snapshots y permisos. Comprueba el proyecto/identidad visible y su despliegue actual; evita sobreescribir trabajo remoto ajeno sin inspección.
2. Abre el editor del proyecto anterior **en el navegador interno**. Si SSO solicita autenticación, **cede el control al usuario para que se autentique**; no solicites ni gestiones contraseñas, OTP o cookies y no intentes eludir controles. Continúa solo cuando la sesión esté disponible.
3. Sincroniza únicamente los archivos realmente modificados con sus nombres de Apps Script correspondientes. Conserva manifiesto, servicios y configuración existente salvo cambio justificado. Verifica el contenido guardado antes de desplegar.
4. Si el cambio debe quedar operativo, actualiza el **despliegue WebApp existente** mediante la interfaz (nueva versión del despliegue pertinente), sin crear un despliegue paralelo por defecto ni alterar URL, ejecutor o acceso. Confirma explícitamente la identidad del despliegue, versión y URL visibles. Si requiere una decisión o autorización de publicación que no esté concedida, solicita esa intervención antes del paso irreversible.
5. Valida la URL desplegada con pruebas de humo de acceso, navegación, vistas clave, administración y publicación/newsletter **sin enviar correos reales ni importar o borrar datos de producción como mera prueba**. Corrobora integridad de la edición y telemetría. Si una prueba necesita una acción real destructiva o un envío, requiere autorización específica.
6. Si el entorno no soporta navegador interno, SSO o despliegue: **no afirmes éxito**; deja cambio local, archivos afectados y pasos concretos pendientes. No sustituyas por `clasp`.

## 6. Gobierno del Google Sheet de producción

**Sheet autorizado:** https://docs.google.com/spreadsheets/d/1JjaqcOe3spKqKxfeeEw5pb4qgUNGaF6BkS0Y8S6WKEI/edit

La fuente **canónica del contrato** son las constantes `NPS_LENS.activitySheet`, `recipientsSheet`, `publicationsSheet` en `00_Config.gs` y `ACTIVITY_HEADERS`, `NEWSLETTER_RECIPIENT_HEADERS`, `PUBLICATION_HEADERS` en los módulos `.gs`; el aprovisionamiento está en `setupNpsLensWebApp` (`30_Administration.gs`). El Sheet almacena estado operativo, **no es el motor de cálculo analítico**.

**Antes y después de cualquier cambio del esquema**, y al publicar cambios que puedan afectarlo:

1. Si hay acceso autorizado, lee pestañas y cabeceras reales desde el Sheet vivo; compara nombres, orden, número y tipos de columnas con el código. La exportación Excel es una **fotografía**, no sustituye la comprobación en vivo.
2. Inventaría todas las pestañas: obligatorias, extras, candidatas a obsoletas, usos/callers y propietarios. No declares una pestaña obsoleta solo porque no aparece en un módulo: verifica referencias, integraciones y operaciones manuales.
3. No borres ni renombres hojas/columnas ni reordenes datos poblados automáticamente. Haz copia/exportación previa, define mapeo y migración idempotente, valida recuentos, claves, referencias y lectura/escritura, y **solicita autorización explícita antes de borrar datos de producción**. Mejor dejar bloqueo visible que perder información.
4. Mantén un único registro central de nombres, contratos y setup; `setupNpsLensWebApp` debe ser seguro al repetirlo y detectar incompatibilidades pobladas sin pérdida silenciosa. Verifica que los roles y restricciones siguen vigentes.
5. Comprueba que no quedan pestañas huérfanas *tras* una migración autorizada y que la WebApp lee/escribe bajo el contrato nuevo. Registra resultados de comprobación y cualquier discrepancia sin maquillar.

**Advertencia fundada en la exportación recibida (noviembre de 2026):** la copia `NPS Lens.xlsx` muestra tres pestañas, pero **dos cabeceras no coinciden con el contrato del código adjunto**:

- `PUBLICACIONES_NPS`: Excel contiene 16 columnas (`buug`, `n1`, `n2`, ...); `PUBLICATION_HEADERS` espera 14 (`owner_support_company`, ...).
- `DESTINATARIOS_NEWSLETTER`: Excel contiene 9 (`buug`, `n1`, ...); `NEWSLETTER_RECIPIENT_HEADERS` espera 8 (`owner_support_company`, ...).
- `ACTIVIDAD_NPS_LENS`: las 11 cabeceras coinciden; varios valores `version` se exportaron como fechas, por lo que **no se puede asumir integridad de tipo/versión** sin inspeccionar formato real.

El código de setup **rechaza cabeceras diferentes cuando hay datos**, en vez de migrarlas. **No ejecutar setup a ciegas** ni asumir que el Sheet activo se corresponde con la exportación: verificar primero configuración (`NPS_LENS_SPREADSHEET_ID`), contrato vivo y plan de migración. La presencia de estas discrepancias es bloqueante para asegurar alineación, aunque no prueba por sí sola un fallo de producción.

## 7. Telemetría y criterios medibles de rendimiento

**Local:** `src/nps_lens/core/telemetry.py`, API `GET /api/telemetry` y `/api/telemetry/export` (`src/nps_lens/api/app.py`): duración, CPU, RSS pico del proceso, bytes, estado y p50/p95/p99. Buffer de 2.000 eventos por defecto. `cpu_ms` es delta de CPU del proceso e incluye concurrencia; `rss_mb` es **pico de memoria del proceso**, no memoria puntual de la petición.

**WebApp:** eventos enviados desde navegador y almacenados en `ACTIVIDAD_NPS_LENS`; `20_Activity.gs` agrega `server_call`, `snapshot_load`, `view`, errores, latencias p95 y versión `vX.Y.Z`, con límites de 50 eventos por lote y 50.000 filas. Es telemetría **con identidad corporativa**, no anónima; respetar privacidad y política corporativa.

Para cambios de rendimiento o con posible coste material:

1. Captura **baseline** y posterior sobre la misma operación, tamaño de datos, filtros, entorno, versión y condiciones comparables. Registra p50/p95/p99, CPU, RSS pico, tiempo de carga y tamaños cuando sean disponibles; en WebApp, `serverP95Ms`/`renderP95Ms` y `snapshot_load`, además de incidencias y recuentos.
2. Divide por operación/ruta y versión. Descarta resultados con volumen ínfimo, caché fría/caliente mezclada o ventanas no comparables. Mide al menos varios ciclos representativos; no inventes umbrales ni porcentajes sin muestra.
3. Si no hay acceso o eventos suficientes, deja **NO MEDIDO** y explica cómo repetir la medición. Nunca presentes una predicción como mejora demostrada.
4. Evita añadir telemetría duplicada o nuevos datos sensibles; instrumenta solo huecos necesarios con límites y limpieza.

## 8. Tests, CI y cierre

Usa los comandos existentes, según el área afectada: `make lint` (Ruff/Black), `make typecheck` (mypy), `make test` (pytest con cobertura mínima del 80 %), `make frontend-test` (Vitest), `make frontend-build` (TypeScript/Vite), `make frontend-e2e` (Playwright), `make ci` (lint + frontend unit/build/E2E). Los workflows independientes de `.github/workflows/` pueden imponer comprobaciones adicionales: revísalos si cambian build, typing o CI. Usa primero tests focalizados para ahorrar recursos, pero no omitas las verificaciones exigidas por el riesgo y CI.

**Criterios de aceptación para cada entrega:** comportamiento solicitado verificado; contratos/consumidores conservados o migrados; tests relevantes adecuados; ausencia de código obsoleto añadido; sin regresiones local/WebApp; seguridad y datos preservados; métricas comparables cuando proceda; y estado explícito de publicación y alineación Sheet.

**Formato de respuesta final (breve):** `Implementado` (qué y dónde) · `Validación` (comandos y resultados) · `WebApp / Sheet` (sin impacto, verificado, publicado o pendiente, con evidencia) · `Rendimiento` (medido o no medido) · `Riesgos/bloqueos`. No pegues logs extensos ni vuelques archivos sin petición expresa.
