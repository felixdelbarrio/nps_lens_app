# Corrección de publicación y auditoría funcional LLM

## Resultado

- Exportar con «Todos» resuelve el último mes disponible dentro de la selección de año/mes. El ZIP guarda el periodo concreto y ambas presentaciones.
- Las brechas comparan las respuestas del mes con todas las respuestas válidas anteriores. El canal selecciona los tópicos; el grupo score no reduce la población del cálculo. Se elimina el caso artificial de base y segmentos a −100.
- Con «Todos», las brechas usan el último mes, coherente con su encabezado, y no el histórico completo.
- Comentarios, exportación, newsletter y presentación descriptiva comparten la aplicación de las asignaciones LLM. Las comparativas por segmentos mantienen la población del sumario.
- Los controles estáticos usan las etiquetas reales del canal y grupos disponibles. La navegación no vuelve a introducir «Web» cuando el dataset contiene «WEB».
- Cada exportación tiene identidad propia. Importar una edición existente se rechaza; importar otra añade una fila y conserva snapshots y presentaciones anteriores. Un bloqueo serializa importaciones; los errores de conversión limpian solamente los archivos nuevos.
- El acceso general permite a todos los usuarios autorizados elegir compañía, mostrando su último snapshot por fecha de generación. La newsletter abre su edición exacta, sin selector de ámbito ni método. Una clave desconocida produce un error, sin abrir otra publicación.
- El selector de compañía está disponible también fuera del sumario. La administración conserva el catálogo histórico para seleccionar la edición de una newsletter.
- La exportación no modifica el dashboard almacenado en caché. La validación causal costosa se ejecuta solo al construir un resultado no cacheado.

## Evidencia

Se trabajó sobre una copia aislada de la base SQLite y configuración de la aplicación instalada; no se modificaron sus clasificaciones ni se enviaron newsletters.

Argentina, agosto de 2026:

| Comprobación | Resultado |
|---|---:|
| Respuestas históricas con clasificación LLM vigente | 16.845 / 16.845 |
| Respuestas del mes, todos los canales | 2.181 |
| Respuestas Web con clasificación completa | 2.135 / 2.135 |
| Incidencias clasificadas en la ventana causal | 2.957 / 2.957 |
| NPS clásico de la base enero–julio | 11,613475 |
| Vistas de tópicos contrastadas con recuentos originales | 12 |
| Vistas de brechas contrastadas entre aplicación y snapshot | 6 |
| Relaciones LLM del periodo verificadas | 82 |
| Comentarios únicos relacionados | 6 |
| Incidencias únicas relacionadas | 82 |
| Diapositivas completa / sin evolución | 10 / 8 |

Se comprobó la conservación de scores, cobertura y vigencia de asignaciones, recuentos por grupo/canal, ausencia de pares duplicados, pertenencia de los IDs a las poblaciones filtradas y rango de confianza. Los tópicos muestran los diez más frecuentes; los empates en el corte pueden seleccionar etiquetas distintas con el mismo volumen. Las asociaciones semánticas siguen presentándose como evidencia de relación, sin afirmar causalidad demostrada.

El ZIP real terminó en 20–23 segundos, aproximadamente 4,54 MB; pico de memoria del proceso de verificación: aproximadamente 1,5 GiB. Estas son mediciones locales, no garantías universales de rendimiento.

Validación: 363 pruebas backend, cobertura 83,27%; 35 pruebas frontend y build correcto. Pruebas adicionales después de los ajustes finales cubren periodo «Todos», controles, caché, identidad, selección por compañía, newsletter exacta, importación aislada y fallo de conversión. Ruff, mypy y revisión del diff. Vista previa de WebApp inspeccionada en navegador: controles WEB/Subpalanca, base 11,6 y brechas no nulas, sin errores registrados.

La auditoría valida el comportamiento del software y la consistencia de las clasificaciones importadas; no certifica que cada juicio semántico del LLM sea correcto. No se ejecutó una importación contra Google Drive/Apps Script real: las pruebas de publicación usaron dobles de Drive, Properties y Sheets.

## Aplicación de la actualización

1. Usar la aplicación macOS recompilada de `build/pyinstaller/macos/dist/nps-lens.app`.
2. Actualizar los archivos del proyecto Apps Script con `build/releases/nps-lens-webapp-20261001.zip` y desplegar una nueva versión de la misma WebApp. Conservar sus propiedades, hoja de publicaciones y carpeta de Drive.
3. Importar el nuevo ZIP de publicación. Cada exportación crea una edición distinta; no eliminar las anteriores utilizadas por newsletters.

El código y el paquete están preparados localmente. La WebApp remota no se ha desplegado desde esta sesión.
