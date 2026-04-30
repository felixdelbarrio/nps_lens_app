# Operación y troubleshooting

---

## 1) Targets Make

- `make setup` — crea `.venv` e instala dependencias
- `make run` — app de escritorio/local
- `make lint` — ruff + black
- `make typecheck` — mypy
- `make test` — pytest + coverage
- `make ci` — lint backend + frontend + E2E
- `make platform CONFIG=...` — batch runner
- `make build` — build PyInstaller (mac/linux)

---

## 2) Configuración (.env)

- Copia `.env.example` → `.env`
- Variables típicas:
  - `NPS_LENS_SERVICE_ORIGIN_BUUG=...`
  - `NPS_LENS_SERVICE_ORIGIN_N1=...` (map JSON)
  - `NPS_LENS_SERVICE_ORIGIN_N2=...` (opcional)
  - `NPS_LENS_UI_POP_YEAR=...` / `NPS_LENS_UI_POP_MONTH=...`
  - `NPS_LENS_UI_SCORE_CHANNEL=...`
  - `NPS_LENS_UI_NPS_GROUP=...` (persistencia interna del Grupo Score)
  - `NPS_LENS_UI_MIN_N_CROSS_COMPARISONS=...`
  - paths de data/knowledge
  - `NPS_LENS_PPT_TEMPLATE=...` (opcional)

**Nota**: cualquier dato sensible debe vivir en `.env` o en secrets de CI, nunca en git.

---

## 3) Performance

- Vite mantiene feedback rápido en frontend y FastAPI sirve la app empaquetada.
- DiskCache reduce recomputes pesados.
- Pushdown temporal (Año/Mes) reduce RAM/CPU.
- La UI no recalcula KPIs ni URLs Helix: consume payloads de `DashboardService`.
- Evita revalidaciones manuales si el estado operativo está `SINCRONIZANDO` o `GENERANDO`.

---

## 4) Problemas frecuentes

### “No hay registros para el contexto” al ingestar Helix
- Verifica que `service_origin_n1_map` está bien parseado (valores sin `[` `]`).
- Revisa que el Excel trae columnas `Servicio Origen - Servicio N1` (se mapea).
- Si el extract ya viene filtrado y no trae Company, se ingesta con WARN.

### Enlaces Helix abren la base sin incidencia
- Revisa que el extract Helix traiga `Record ID`, `workItemId` o `InstanceId`.
- La app nunca debe construir `helix_base_url + Incident Number`.
- Si existe una URL explícita válida, se usa; si no, se construye con `Record ID`.

### KPIs de Ámbito de Análisis cambian al tocar Canal/Grupo
- Es una regresión: el Sumario del Periodo solo debe depender de `SERVICE CONTAINER` + `PERIOD CONTAINER`.
- Canal y Grupo Score solo afectan Analítica NPS Térmico, Incidencias ↔ NPS, datos tabulares filtrados y reportes causales.

### Warnings de pandas (groupby observed)
- El código fija `observed=True` donde aplica.

### Estado operativo bloquea acciones
- `SINCRONIZANDO` aparece durante cargas/revalidaciones.
- `GENERANDO` aparece durante el reporte PPT.
- Si queda bloqueado, revisa errores de API y que no haya una mutación de carga/reproceso pendiente.

---

## 5) Logs
- La ingesta devuelve `issues` (INFO/WARN/ERROR) visibles en UI.
- Evita imprimir datasets completos.
