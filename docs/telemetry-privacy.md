# Telemetría y alertas DLP

La alerta del 8 de septiembre de 2026 muestra como destino
`telemetry.individual.githubcopilot.com/telemetry`. El código de telemetría de
NPS Lens no utiliza ese destino. La captura no identifica el proceso emisor ni
el contenido bloqueado: no basta para confirmar que sea un falso positivo.

## Flujos de NPS Lens

- Aplicación local: métricas de peticiones en una cola en memoria de hasta 2000
  eventos. Recoge tiempos, CPU, memoria, tamaños, estados y plantillas de ruta,
  sin parámetros de consulta ni cuerpos de petición o respuesta. Las rutas no
  reconocidas se agrupan como `<unmatched>`. La consulta usa la API de la propia
  aplicación; la exportación administrativa guarda un JSON en Descargas. No
  hay un envío automático de estas métricas a un proveedor externo.
- WebApp: el navegador envía eventos por `google.script.run` al proyecto Apps
  Script, que los guarda en `ACTIVIDAD_NPS_LENS` de la hoja configurada. Conserva
  correo corporativo y sesión para adopción, pantallas, tiempos y estados; por
  tanto, **no es telemetría anónima**. El detalle admite operaciones conocidas,
  no mensajes de excepción. El registro está limitado a 50000 filas y el informe
  requiere permisos de administrador. Google Workspace sigue siendo un destino
  de red sujeto a las políticas corporativas.

Los cambios de minimización se aplican a eventos nuevos. No borran los mensajes
que ya pudieran existir en la hoja ni en exportaciones anteriores. La versión
publicada de Apps Script y la aplicación empaquetada deben actualizarse para
incorporarlos.

## Cómo abordar la alerta mostrada

1. Solicitar a Seguridad/IT la identificación del proceso emisor y la regla DLP
   correspondiente a la hora y destino indicados. No adjuntar datos de clientes
   a la solicitud.
2. Si el origen es VS Code/Copilot, configurar en los ajustes de usuario
   `"telemetry.telemetryLevel": "off"`, o aplicar la política corporativa
   `TelemetryLevel`. Revisar también las extensiones: algunas no respetan ese
   ajuste. Esta configuración afecta al editor, no a las métricas de NPS Lens.
3. Repetir la operación y comprobar con IT qué conexiones persisten. Cualquier
   autorización del destino debe corresponder a la política corporativa y al
   producto/cuenta aprobados.

No se garantiza la ausencia de alertas DLP por cambiar el contenido de las
métricas: una política puede bloquear un destino independientemente del payload.
No se deben ocultar, redirigir ni codificar envíos para eludir la inspección.

Referencias oficiales:
- [Destinos de Copilot](https://docs.github.com/en/copilot/reference/copilot-allowlist-reference)
- [Telemetría de VS Code y extensiones](https://code.visualstudio.com/docs/configure/telemetry)
- [Políticas de telemetría corporativas](https://code.visualstudio.com/docs/enterprise/telemetry)
