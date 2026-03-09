# Operación y troubleshooting

---

## 1) Targets Make

- `make setup` — crea `.venv` e instala dependencias
- `make run` — Streamlit
- `make ci` — ruff + black + mypy + pytest
- `make platform CONFIG=...` — batch runner
- `make build` — build PyInstaller (mac/linux)

---

## 2) Configuración (.env)

- Copia `.env.example` → `.env`
- Variables típicas:
  - `NPS_LENS_SERVICE_ORIGIN_BUUG=...`
  - `NPS_LENS_SERVICE_ORIGIN_N1=...` (map JSON)
  - `NPS_LENS_SERVICE_ORIGIN_N2=...` (opcional)
  - `NPS_LENS_UI_MIN_N_CROSS_COMPARISONS=...`
  - paths de data/knowledge
  - `NPS_LENS_PPT_TEMPLATE=...` (opcional)

**Nota**: cualquier dato sensible debe vivir en `.env` o en secrets de CI, nunca en git.

---

## 3) Performance

- Watchdog acelera el hot‑reload en Streamlit.
- DiskCache reduce recomputes pesados.
- Pushdown temporal (Año/Mes) reduce RAM/CPU.

---

## 4) Problemas frecuentes

### “No hay registros para el contexto” al ingestar Helix
- Verifica que `service_origin_n1_map` está bien parseado (valores sin `[` `]`).
- Revisa que el Excel trae columnas `Servicio Origen - Servicio N1` (se mapea).
- Si el extract ya viene filtrado y no trae Company, se ingesta con WARN.

### Warnings de pandas (groupby observed)
- El código fija `observed=True` donde aplica.

### Dark mode: controles blancos
- `ui/theme.py` contiene overrides a BaseWeb popovers.
- Si cambias la versión de Streamlit, revisa selectores `data-baseweb`.

---

## 5) Logs
- La ingesta devuelve `issues` (INFO/WARN/ERROR) visibles en UI.
- Evita imprimir datasets completos.
