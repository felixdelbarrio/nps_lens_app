function recordTelemetry(events) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const rows = Array.isArray(events) ? events.slice(0, 50) : [];
  if (!rows.length) return {accepted: 0};
  const spreadsheetId = _property_('NPS_LENS_SPREADSHEET_ID');
  if (!spreadsheetId) return {accepted: 0};
  const sheet = SpreadsheetApp.openById(spreadsheetId).getSheetByName(NPS_LENS.telemetrySheet);
  if (!sheet) throw new Error('Ejecuta setupNpsLensWebApp antes de registrar telemetría.');
  const now = new Date();
  const values = rows.map(event => [
    now, viewer.email, String(event.type || 'view'), String(event.screen || ''),
    Number(event.durationMs || 0), String(event.detail || '').slice(0, 1000), NPS_LENS.version
  ]);
  LockService.getScriptLock().waitLock(5000);
  try {
    sheet.getRange(sheet.getLastRow() + 1, 1, values.length, values[0].length).setValues(values);
    const excess = sheet.getLastRow() - NPS_LENS.maxTelemetryRows - 1;
    if (excess > 0) sheet.deleteRows(2, excess);
  } finally {
    LockService.getScriptLock().releaseLock();
  }
  return {accepted: values.length};
}

function getTelemetry() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const spreadsheetId = _property_('NPS_LENS_SPREADSHEET_ID');
  if (!spreadsheetId) return {events: 0, screens: [], errors: 0, p95Ms: 0};
  const sheet = SpreadsheetApp.openById(spreadsheetId).getSheetByName(NPS_LENS.telemetrySheet);
  const values = sheet && sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow() - 1, 7).getValues() : [];
  const durations = values.map(row => Number(row[4] || 0)).filter(value => value > 0).sort((a, b) => a - b);
  const screenCounts = {};
  values.forEach(row => { const key = String(row[3] || 'sin-pantalla'); screenCounts[key] = (screenCounts[key] || 0) + 1; });
  return {
    events: values.length,
    errors: values.filter(row => row[2] === 'error').length,
    p95Ms: durations.length ? durations[Math.min(durations.length - 1, Math.floor(durations.length * .95))] : 0,
    screens: Object.keys(screenCounts).map(screen => ({screen, visits: screenCounts[screen]})).sort((a, b) => b.visits - a.visits)
  };
}
