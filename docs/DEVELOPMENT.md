# Desarrollo (contribución)

---

## Estándares
- Python: **3.9.13**
- Formato: `black`
- Lint: `ruff`
- Tipado: `mypy` (strict, con exclusiones pragmáticas para UI/Pandas-heavy)
- Tests: `pytest` + coverage

---

## Flujo recomendado
1) `make setup`
2) `make lint`
3) `make typecheck`
4) `make test`
5) `make ci`
6) PR pequeño y trazable

---

## Estilo de código
- Sin retrocompatibilidad silenciosa: si el pipeline cambia, **fail-fast** con mensaje claro.
- Evitar lógica de negocio en UI: centralizar filtros, KPIs, payloads y URLs Helix en `DashboardService` y helpers de dominio.
- Añadir docs cuando se añadan módulos/capacidades.

## Reglas de dominio vigentes
- `SERVICE CONTAINER` gobierna `service_origin`, `service_origin_n1` y `service_origin_n2`.
- `PERIOD CONTAINER` gobierna `pop_year` y `pop_month` para toda la app.
- `Canal` y `Grupo Score` se aplican a Analítica NPS Térmico, Incidencias ↔ NPS, tablas y reportes causales; no deben alterar el Ámbito de Análisis del Sumario del Periodo.
- Usar `Score` para medias/valores 0-10 y reservar `NPS clásico` para `% promotores - % detractores`.
- No construir enlaces Helix fuera de `nps_lens.domain.helix_links`.
