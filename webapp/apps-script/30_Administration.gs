function getAdministration() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const edition = _publishedEdition_();
  return {
    version: NPS_LENS.version,
    generatedAt: edition.generated_at || '',
    reportUrl: getReportUrl(),
    telemetry: getTelemetry()
  };
}

function setupNpsLensWebApp(spreadsheetId, adminEmails) {
  const properties = PropertiesService.getScriptProperties();
  properties.setProperty('NPS_LENS_SPREADSHEET_ID', String(spreadsheetId || '').trim());
  properties.setProperty(NPS_LENS.adminEmailsProperty, String(adminEmails || '').trim());
  const book = SpreadsheetApp.openById(String(spreadsheetId).trim());
  let sheet = book.getSheetByName(NPS_LENS.telemetrySheet);
  if (!sheet) sheet = book.insertSheet(NPS_LENS.telemetrySheet);
  if (!sheet.getLastRow()) sheet.appendRow(['timestamp', 'type', 'screen', 'duration_ms', 'detail', 'version']);
  sheet.setFrozenRows(1);
  return {ok: true, sheet: NPS_LENS.telemetrySheet};
}
