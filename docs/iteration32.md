# Marco de clasificación transversal y persistencia por compañía

Taxonomy Studio utiliza un único selector, situado antes de las pestañas. Su marco
se aplica al análisis estático, los intercambios LLM, Insights, presentaciones y
snapshots. Se eliminan la selección LLM independiente y la selección distinta para
snapshots. Solo aparecen catálogos disponibles; sin ellos se deshabilita la
clasificación, manteniendo disponibles la creación manual y el descubrimiento.

La preferencia se guarda por Owner Support Company en el JSON
`NPS_LENS_CLASSIFICATION_FRAMEWORKS` del `.env`, conservando el resto de preferencias.
El recorrido Playwright utiliza un `.env` aislado en `.playwright-data`.

Los catálogos sobreviven a cambios del corpus. Los comentarios mantienen sus
asignaciones por identidad y huella del texto; los nuevos o modificados quedan
pendientes. Helix conserva una identidad persistente por revisión de taxonomía,
anclada al ámbito inicial, sin recalcularla al añadir evidencia NPS. Valida cada
incidencia por su huella y registra las huellas de sus comentarios enlazados: un
cambio de evidencia invalida solo las incidencias dependientes. Las huellas NPS
se calculan únicamente para comentarios enlazados y se reutilizan en la lectura.

El indicador «Con más de una categoría» cuenta registros con una principal y al
menos una adicional. El texto explicativo se comparte entre comentarios e
incidencias. Los adicionales no multiplican los totales ni el NPS.

La respuesta Helix adjunta usaba `lever/sublever` y `secondary_classifications`
en lugar de IDs `primary/secondary`. Se reforzaron las instrucciones de validación
final. La conversión puntual del archivo adjunto conserva sus decisiones, IDs,
orden, manifiesto y vínculos; no se incorpora un importador de formatos antiguos.
La importación en una base temporal con el catálogo y evidencia originales aceptó
291 incidencias y 3 vínculos. No se importaron resultados en la base del usuario.

Validación: 360 pruebas backend (81,79 % de cobertura), 35 frontend, recorrido
E2E, compilación, tipado de 111 archivos y comprobaciones de estilo. Las pruebas
incluyen WebApp, alternancia de marcos, persistencia tras reinicio, nuevos registros,
invalidación de evidencia enlazada y selector vacío.

La revisión posterior del flujo LLM y sus nuevos contratos se documenta en
[iteration32-semantic-contract.md](iteration32-semantic-contract.md).
