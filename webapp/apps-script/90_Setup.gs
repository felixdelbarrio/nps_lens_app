function onOpen() {
  SpreadsheetApp.getUi().createMenu('NPS Lens')
    .addItem('Validar configuración', 'getAdministration')
    .addItem('Diagnosticar acceso', 'diagnoseNpsLensAccess')
    .addItem('Preparar administración', 'setupFromActiveSpreadsheet')
    .addToUi();
}

function setupFromActiveSpreadsheet() {
  const active = SpreadsheetApp.getActiveSpreadsheet();
  return setupNpsLensWebApp(active ? active.getId() : '', '');
}
