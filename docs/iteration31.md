# Iteración 31 — intercambios de clasificación

Se mantiene el flujo Proyecto ChatGPT + ZIP, sin APIs de modelos ni servicios nuevos.
Los cambios se limitan a classifier/Helix y su UI local. Designer y normalizer
conservan contratos y tamaños de lote. La WebApp, analytics y reporting consumen
los mismos formatos internos con Palanca/Subpalanca, secundarios y vínculos.

## Cambios

- `classification_protocol.py`: IDs de categoría deterministas, validación compartida
  y límites específicos: 1.000 elementos / 300.000 bytes por lote; 4.000 representantes
  classifier y 2.000 incidencias Helix por intercambio.
- `taxonomy_exchange.py`: vacíos resueltos solo con la reserva exacta disponible,
  deduplicación por texto exacto, propagación a claves individuales y persistencia
  común para asignaciones locales/importadas. Los grupos sobreviven al reinicio.
- `helix_exchange.py`: transporte compacto de categorías y evidencia, sin deduplicar
  ni preclasificar incidencias; conserva scopes, huellas, rationale, confidence y links.
- `taxonomy_discovery.py`: eliminados modelos y validadores del protocolo reemplazado.
- `taxonomy_prompts.py`: Python solo para operaciones mecánicas, datos no confiables,
  decisiones semánticas sin reglas de keywords y continuación antes de un parcial.
- `api/app.py`: invalida las cachés tras exportar comentarios porque puede resolver vacíos.
- `TaxonomyProject.tsx`, `HelixClassifier.tsx`, `classificationExchange.ts`: progreso
  actualizado y siguiente ZIP automático mediante el exportador existente. Un fallo
  de exportación conserva la importación y permite reintentar manualmente.

Protocolos: `nps-lens-comments/3` (classifier), `nps-lens-helix/3`.
Los ZIP antiguos de esos dos proyectos se rechazan; las asignaciones ya persistidas
se reutilizan si siguen siendo válidas. No hay conversores de compatibilidad.

## Reproducción del ejemplo adjunto

Exportación aislada, con una base temporal y los textos/catálogo del primer ZIP
classifier (`e03239…`), sin modificar datos del usuario ni llamar a ningún modelo:

| Medida | Antes | Después |
| --- | ---: | ---: |
| Elementos enviados | 16.845 | 3.878 representantes |
| Lotes | 85 | 4 |
| Tamaño ZIP | 236.574 bytes | 132.817 bytes |
| Vacíos resueltos localmente | 0 | 12.488 |

La exportación nueva tardó 0,24 s de pared / 0,23 s de CPU en esta ejecución local.
Es una medición del transporte, no de la latencia ni precisión de ChatGPT.
Las tres respuestas parciales adjuntas contienen un único lote cada una.

## Uso y límites

Sustituir las instrucciones de los Proyectos ChatGPT por las de la app y exportar
ZIP nuevos. ChatGPT aún puede encontrar límites reales de sesión y devolver parciales;
la aplicación no automatiza el envío ni abre ChatGPT. Se mantienen las protecciones
ZIP, el rechazo explícito de elementos excesivos y toda la evidencia NPS de Helix.

## Validación

- `make lint`: correcto (ruff y black).
- `make typecheck`: correcto, 111 archivos de backend.
- `make test`: 345 pruebas aprobadas, cobertura 84,36 %, incluidos los tests WebApp.
- Suite focalizada final: 66 pruebas aprobadas (intercambio, iteraciones 28, 30 y 31),
  incluyendo cuatro casos adicionales de conflictos e invalidación por lente.
- `make frontend-test`: 36 pruebas aprobadas; siete casos cubren la continuación,
  fallo de exportación, finalización y resolución local sin ZIP.
- `make frontend-build`: TypeScript y Vite correctos.
- `git diff --check`: correcto.

Para backend se proporcionó `NPS_LENS_SERVICE_ORIGIN_BUUG` al proceso de pruebas,
porque el entorno de shell no tenía esa variable obligatoria. No se modificó `.env`.
No se ha ejecutado una sesión real de ChatGPT ni se garantiza cuántos lotes completará;
la reproducción del adjunto valida el ahorro y el contrato de transporte.

## Simplificación posterior del intercambio

La descarga prepara ahora todos los pendientes en una carpeta con ZIP numerados,
un lote por archivo, tanto para comentarios como para Helix. Sustituye los topes
de 4.000/2.000 por intercambio y la generación automática después de importar.
Se mantienen 1.000 elementos / 300.000 bytes por lote y la evidencia NPS completa
en cada ZIP Helix. Las respuestas se pueden importar en cualquier orden; la
retención conserva tres series completas. El contrato vigente y las instrucciones
de uso están en `taxonomy-project-instructions.md`.

Validación de esta simplificación: `make ci` correcto (34 pruebas frontend,
compilación y recorrido E2E); `make typecheck` correcto; `make test`: 357 pruebas
aprobadas, cobertura 84,31 %, incluidas las regresiones de WebApp. Ocho pruebas
nuevas cubren series completas, importación fuera de orden tras reinicio,
reexportación de pendientes, validación de manifiestos y fallo atómico de escritura.
