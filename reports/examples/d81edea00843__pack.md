# LLM Deep-Dive Pack — Ejemplo: Palanca con gap vs global

## Contexto
- **geo**: MX
- **channel**: Senda
- **driver_dim**: Palanca
- **driver_val**: Banca
- **period**: 2026-01-01..2026-02-02

## Métricas clave
- **NPS slice**: 54.7
- **% Detractores**: 0.156
- **N**: 276

## Evidencia cuantitativa
```json
{
  "n": 276,
  "overall_nps": 60.3,
  "slice_nps": 54.710144927536234,
  "gap_vs_overall": 5.589855072463763
}
```

## Evidencia cualitativa (muestras)
```json
{
  "verbatims": [
    "Muy mala atencion en El banco ...estan mal organizations y normalmente estan 2 cajeros atendiendo por Este Motivo se tarda uno mas de 1 hora en ser at",
    "buen servicio",
    "Tuve un inconveniente y el asesor telefónico me llevó de la mano para resolver con éxito mi problema",
    "acceso al sucursal y estacionamiento no es aceptable.",
    "Me da buen servicio y cumple con mis expectativas como Banco",
    "BUEN SERVICIO",
    "Muy buena atención y rapidez en los procesos",
    "Son el banco menos malo de Mexico",
    "BUEN SERVICIO",
    "servicio"
  ]
}
```

## Hipótesis
- La palanca seleccionada presenta un gap vs global. Revisar incidents y fricciones en el journey asociado.

## Preguntas sugeridas
- ¿Qué subpalancas dentro de esta palanca concentran detractores?
- ¿Qué temas emergen en los verbatims? ¿Se alinean con incidencias/reviews?
- ¿Hubo cambios/release/incidents en la ventana temporal que expliquen el gap?

## Acciones sugeridas
- Profundizar por Subpalanca y UsuarioDecisión con foco en detractores.
- Cruzar con incidencias por ventana temporal ±3 días y sistema.
- Instrumentar evento en el touchpoint más probable y crear alerta semanal.
