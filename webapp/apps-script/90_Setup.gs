function onOpen() {
  SpreadsheetApp.getUi().createMenu('NPS Lens')
    .addItem('Validar configuración', 'getAdministration')
    .addItem('Diagnosticar acceso', 'diagnoseNpsLensAccess')
    .addItem('Preparar administración', 'setupFromActiveSpreadsheet')
    .addToUi();
}

function setupFromActiveSpreadsheet() {
  const email = Session.getActiveUser().getEmail();
  return setupNpsLensWebApp(SpreadsheetApp.getActive().getId(), email);
}
