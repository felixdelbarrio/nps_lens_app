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
- Evitar lógica de negocio en UI: centralizar en `src/nps_lens/`.
- Añadir docs cuando se añadan módulos/capacidades.
