# Auditoría de identidad causal y equivalencias — 8 septiembre 2026

## Reproducción local

Contexto: BBVA México / ENTERPRISE WEB, canal Web, todos los periodos locales, política de enlace de similitud mínima 0,15 y ventana máxima de 90 días. Se ha auditado el flujo compartido por el dashboard y PowerPoint: resolución de taxonomía → cruce Helix/VoC → catálogo → asignación de entidades → series → cadenas de evidencia.

Antes: 1.986 pares enlazados, 8 journeys y **solo 4 nombres distintos**.

| Etiqueta anterior | Repeticiones | Vínculos por cluster |
| --- | ---: | --- |
| Funcionamiento Continuo / Pagos/ Transferencias | 3 | 376, 310, 190 |
| Funcionamiento Continuo / No funciona bien/ falla | 2 | 314, 135 |
| Uso / Edo de Cuenta | 2 | 275, 225 |
| Pagos/ Transferencias / Problemas con transferencias | 1 | 161 |

Estas cifras no describían ocho recorridos distintos. MiniBatchKMeans forzaba un número de grupos entre 2 y 8; cada grupo recibía una etiqueta a partir de sus categorías dominantes. Así podía dividir una misma ruta y mezclar otras bajo su nombre. Los IDs incorporaban la posición del ranking y cambiaban al cambiar el volumen.

Después: **23 rutas distintas, cero etiquetas o IDs duplicados y 1.986 pares conservados**, tanto en catálogo como en detalle. Cada tema tiene una única asignación. El resultado no es «fusionar los ocho en cuatro»: las etiquetas dominantes anteriores ocultaban otros recorridos presentes en la evidencia.

## Decisión funcional

La identidad de un journey roto es la pareja **Palanca/Subpalanca de la taxonomía activa**, respaldada por vínculos Helix/VoC válidos. No se fuerza ningún número de journeys. Los IDs se derivan de la pareja completa, no del título recortado ni del ranking. TF-IDF sigue proporcionando palabras descriptivas y cohesión de la evidencia; deja de decidir particiones arbitrarias de una misma ruta.

Esto cambia explícitamente el criterio del modo «Journeys rotos»: agrupación por ruta de la taxonomía activa, no descubrimiento libre de clusters textuales. La descripción del método también se ha actualizado. El modo por subpalanca sigue agrupando transversalmente por touchpoint: por eso da 22 entidades mientras las rutas completas dan 23.

No se declara equivalencia semántica entre toda mención de transferencias: problemas con transferencias, límites, pagos o actualización de movimientos pueden ser recorridos distintos. Una asociación textual sigue siendo evidencia asociativa, no una demostración experimental de causalidad.

## Deficiencias corregidas

1. **Fragmentación y nombres repetidos:** identidad por ruta y asignación atómica, sin límite artificial de 8.
2. **Inconsistencia entre detalle y evolución:** catálogo, mapa de temas y cadenas comparten la asignación. El modo ejecutivo reutiliza la entidad ya resuelta en lugar de clasificar cada pareja de nuevo.
3. **Votos fragmentados por metadatos:** se agregan por entidad antes de elegir al ganador; servicio, palanca o touchpoint secundarios no reducen sus votos. Los desempates son deterministas.
4. **Promedio NPS sesgado por multiplicidad de vínculos:** catálogo y cadenas cuentan una vez cada respuesta, aunque enlace con varias incidencias.
5. **Agregación duplicada de series:** la función de journeys delega en el agregador común. Los promedios desconocidos no se convierten en puntuaciones cero al fusionar temas.
6. **Cruces que podían multiplicar filas:** referencias idénticas se deduplican antes del join; referencias contradictorias con el mismo ID fallan mediante validación de cardinalidad, en lugar de escoger arbitrariamente. Los enlaces huérfanos no crean evidencia ficticia.
7. **Nulos convertidos en texto:** se rellenan antes de convertir a cadena. Se reutilizan los constructores centrales de temas NPS y Helix.
8. **Metadatos de catálogo ocultos por sufijos del merge:** el detalle recibe explícitamente los metadatos resueltos del journey.
9. **Pérdida de evidencia sin vocabulario TF-IDF:** las etiquetas de un carácter no eliminan pares válidos; se conserva la ruta con cohesión cero.
10. **Equivalencias incompletas y sugerencias no deterministas:** las sugerencias comparan también con los grupos configurados, incluso si solo aparece una variante en los datos. No se elige un destino arbitrario si varios grupos coinciden por formato.
11. **Dimensiones Helix ausentes:** se admiten equivalencias para Product Categorization Tier 1/2/3, usadas por el constructor central de temas de incidencias.
12. **Caché de Helix y snapshots:** la clave depende de la firma de las equivalencias efectivas del contexto, no solo de la fecha del fichero local.
13. **Trabajo innecesario:** los otros métodos ya no calculan el catálogo de journeys rotos que no consumen.

## Equivalencias locales

Se mantiene el contrato existente: las equivalencias son **exactas, explícitas y por dimensión**. SOURCE conserva las categorías de origen; NORMALIZED aplica el registro; las taxonomías generadas y snapshots siguen su política de resolución. No se introducen coincidencias difusas ni sustituciones en comentarios o descripciones.

Se añadieron al registro local 10 variantes de formato observadas, correspondientes a estos destinos ya configurados:

| Dimensión | Variantes añadidas | Destino |
| --- | --- | --- |
| nps.Palanca | Agregar Funcionalidad | Agregar funcionalidad |
| nps.Palanca | Funcionamiento Continuo | Funcionamiento continuo |
| nps.Palanca | Pagos/ Transferencias | Pagos/transferencias |
| nps.Subpalanca | Agregar Funcionalidad | Agregar funcionalidad |
| nps.Subpalanca | Fallas en el Login | Fallas en el login |
| nps.Subpalanca | Interface/ Actualización | Interface/Actualización |
| nps.Subpalanca | No funciona bien/ falla | No funciona bien/falla |
| nps.Subpalanca | Pagos/ Transferencias | Pagos/transferencias |
| nps.Subpalanca | Practicidad/ Facilidad de uso; Practicidad/ facilidad de uso | Practicidad/Facilidad de uso |

El registro `data/config/equivalences.json` es configuración local ignorada por Git. Esta tabla documenta su cambio para replicarlo en otra instalación mediante la administración de equivalencias. La copia previa queda en `/tmp/nps-equivalences-before-audit.json` durante esta sesión. No se han modificado comentarios ni registros originales.

## Validación

El auditor reproducible comprueba identidad, conservación de pares, entidades compartidas entre mapa y detalle y conservación de respuestas/incidencias en las series de los temas asignados.

| Método | Entidades | Pares asignados | Comprobaciones locales |
| --- | ---: | ---: | --- |
| Journeys rotos | 23 | 1.986 | Todas correctas |
| Subpalanca | 22 | 1.986 | Todas correctas |
| Palanca | 4 | 1.986 | Todas correctas |
| Servicio origen N2 | 3 | 1.986 | Todas correctas |
| Journeys ejecutivos | 3 | 1.986 | Todas correctas |

La unidad de evidencia es el par incidencia/respuesta. Una incidencia puede respaldar varios recorridos: sus conteos por recorrido no deben sumarse para obtener incidencias globales únicas. Las series conservan las asignaciones de incidencias del motor de enlace; no representan el número de pares semánticos.

Reproducción sin exportar comentarios ni descripciones:

```sh
.venv/bin/python scripts/audit_causal_journeys.py \
  --service-origin 'BBVA México' \
  --service-origin-n1 'ENTERPRISE WEB' \
  --output /tmp/nps-causal-audit.json
```

Se añadieron 13 regresiones de atomicidad, estabilidad ante permutaciones y nuevas rutas, duplicados, nulos, referencias contradictorias, equivalencias explícitas, votos por entidad, conservación de series, medias e identidad ejecutiva. La suite existente cubre además API, taxonomías y generación de informes. Ejecución final estable: **208 tests aprobados y 82,96% de cobertura**, por encima del 80% requerido (295,27 s).

Los procesos de backend ya abiertos deben reiniciarse para cargar el código. Los informes y publicaciones exportados anteriormente son snapshots: deben regenerarse para reflejar el nuevo cálculo.

Comprobaciones estáticas: Ruff pasa en todo el repositorio; mypy pasa en los módulos tipados modificados; Black pasa en todos los archivos modificados. Black global detecta dos incumplimientos preexistentes en `services/analytics/kpis_service.py` y `tests/api/test_dashboard_api.py`, reproducidos también contra HEAD y ajenos a esta corrección.
