const ACTIVITY_HEADERS = Object.freeze([
  'timestamp', 'user_email', 'session_id', 'event_name', 'area', 'section',
  'view', 'duration_ms', 'status', 'detail', 'version'
]);

function recordActivityEvents(events) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const batch = Array.isArray(events) ? events.slice(0, NPS_LENS.maxActivityBatch) : [];
  if (!batch.length || !_property_('NPS_LENS_SPREADSHEET_ID')) return {accepted: 0};
  const now = new Date();
  const values = batch.map(event => [now, viewer.email, _cleanText_(event.sessionId, 80),
    _cleanText_(event.name || 'view', 80), _cleanText_(event.area, 60), _cleanText_(event.section, 80),
    _cleanText_(event.view, 100), Math.max(0, Math.round(Number(event.durationMs) || 0)),
    _cleanText_(event.status || 'ok', 30), _cleanText_(event.detail, 500), NPS_LENS.version]);
  const lock = LockService.getScriptLock();
  lock.waitLock(5000);
  try {
    const sheet = _sheet_(NPS_LENS.activitySheet);
    sheet.getRange(sheet.getLastRow() + 1, 1, values.length, ACTIVITY_HEADERS.length).setValues(values);
    const excess = sheet.getLastRow() - NPS_LENS.maxActivityRows - 1;
    if (excess > 0) sheet.deleteRows(2, excess);
  } finally { lock.releaseLock(); }
  return {accepted: values.length};
}

function getActivityReport(request) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const options = request || {};
  const days = Math.min(365, Math.max(1, Number(options.days) || 30));
  const email = _cleanText_(options.email, 180).toLowerCase();
  const cutoff = Date.now() - days * 86400000;
  const sheet = _sheet_(NPS_LENS.activitySheet);
  const values = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow() - 1, ACTIVITY_HEADERS.length).getValues() : [];
  const selected = values.filter(row => {
    const timestamp = row[0] instanceof Date ? row[0].getTime() : new Date(row[0]).getTime();
    return timestamp >= cutoff && (!email || String(row[1] || '').toLowerCase() === email);
  });
  const durations = selected.map(row => Number(row[7]) || 0).filter(Boolean).sort((a, b) => a - b);
  const counts = column => {
    const result = {};
    selected.forEach(row => { const key = String(row[column] || 'sin-dato'); result[key] = (result[key] || 0) + 1; });
    return Object.keys(result).map(name => ({name, events: result[name]})).sort((a, b) => b.events - a.events);
  };
  const users = counts(1).map(item => ({email: item.name, events: item.events}));
  return {
    generatedAt: new Date().toISOString(), periodDays: days,
    summary: {events: selected.length, users: users.length,
      sessions: new Set(selected.map(row => String(row[2] || '')).filter(Boolean)).size,
      errors: selected.filter(row => row[8] === 'error').length,
      p95Ms: durations.length ? durations[Math.min(durations.length - 1, Math.floor(durations.length * .95))] : 0},
    users, actions: counts(3), screens: counts(4).slice(0, 30),
    events: selected.slice(-1000).reverse().map(row => ({
      timestamp: row[0] instanceof Date ? row[0].toISOString() : String(row[0] || ''),
      user: String(row[1] || ''), session_id: String(row[2] || ''), name: String(row[3] || ''),
      area: String(row[4] || ''), section: String(row[5] || ''), view: String(row[6] || ''),
      duration_ms: Number(row[7]) || 0, status: String(row[8] || ''), detail: String(row[9] || ''), version: String(row[10] || '')}))
  };
}

function exportActivityReport(request) {
  const report = getActivityReport(request);
  const payload = {schema_version: '2.0',
    app: {name: 'NPS Lens WebApp', version: NPS_LENS.version, runtime: 'Google Apps Script'},
    privacy: {authenticated_users_recorded: true, customer_data_recorded: false, filter_values_recorded: false}, report};
  return {fileName: 'nps-lens-webapp-diagnostico-' + Utilities.formatDate(new Date(), 'UTC', 'yyyyMMdd-HHmmss') + '.json', content: JSON.stringify(payload)};
}
