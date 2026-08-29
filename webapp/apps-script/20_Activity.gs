const ACTIVITY_HEADERS = Object.freeze([
  'timestamp', 'user_email', 'session_id', 'event_name', 'area', 'section',
  'view', 'duration_ms', 'status', 'detail', 'version'
]);

function recordActivityEvents(events) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const batch = Array.isArray(events) ? events.slice(0, NPS_LENS.maxActivityBatch) : [];
  if (!batch.length || !_property_('NPS_LENS_SPREADSHEET_ID')) return {accepted: 0};
  const now = Date.now();
  const values = batch.map(event => {
    const occurredAt = new Date(event.occurredAt || now);
    const timestamp = Number.isFinite(occurredAt.getTime()) && occurredAt.getTime() <= now + 300000 && occurredAt.getTime() >= now - 604800000
      ? occurredAt : new Date(now);
    return [timestamp, viewer.email, _cleanText_(event.sessionId, 80),
    _cleanText_(event.name || 'view', 80), _cleanText_(event.area, 60), _cleanText_(event.section, 80),
    _cleanText_(event.view, 100), Math.max(0, Math.round(Number(event.durationMs) || 0)),
    _cleanText_(event.status || 'ok', 30), _cleanText_(event.detail, 500), 'v' + NPS_LENS.version];
  });
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
  const digest = Utilities.base64EncodeWebSafe(Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, days + '|' + email)).slice(0,24);
  const cache = CacheService.getScriptCache(), cacheKey = 'activity-' + NPS_LENS.version + '-' + digest;
  const cached = cache.get(cacheKey);
  if (cached) return JSON.parse(cached);
  const cutoff = Date.now() - days * 86400000;
  const sheet = _sheet_(NPS_LENS.activitySheet);
  const values = sheet.getLastRow() > 1 ? sheet.getRange(2, 1, sheet.getLastRow() - 1, ACTIVITY_HEADERS.length).getValues() : [];
  const selected = values.filter(row => {
    const timestamp = row[0] instanceof Date ? row[0].getTime() : new Date(row[0]).getTime();
    return timestamp >= cutoff && (!email || String(row[1] || '').toLowerCase() === email);
  });
  const percentile95 = rows => {
    const durations = rows.map(row => Number(row[7]) || 0).filter(Boolean).sort((a, b) => a - b);
    return durations.length ? durations[Math.min(durations.length - 1, Math.floor(durations.length * .95))] : 0;
  };
  const counts = (source, column) => {
    const result = {};
    source.forEach(row => { const key = String(row[column] || 'sin-dato'); result[key] = (result[key] || 0) + 1; });
    return Object.keys(result).map(name => ({name, events: result[name]})).sort((a, b) => b.events - a.events);
  };
  const performanceVersion = NPS_LENS.version.split('.').slice(0, 2).join('.');
  const performanceEvents = selected.filter(row => String(row[10] || '').indexOf('v' + performanceVersion + '.') === 0);
  const viewEvents = performanceEvents.filter(row => row[3] === 'view');
  const serverCalls = performanceEvents.filter(row => row[3] === 'server_call');
  const users = counts(selected, 1).map(item => ({email: item.name, events: item.events}));
  const report = {
    generatedAt: new Date().toISOString(), periodDays: days,
    summary: {events: selected.length, performanceVersion, performanceEvents: performanceEvents.length,
      historicalEvents: selected.length - performanceEvents.length, users: users.length,
      sessions: new Set(selected.map(row => String(row[2] || '')).filter(Boolean)).size,
      errors: performanceEvents.filter(row => row[8] === 'error').length,
      serverP95Ms: percentile95(serverCalls), renderP95Ms: percentile95(viewEvents)},
    users, actions: counts(selected, 3), screens: counts(viewEvents, 4).slice(0, 30),
    slowCalls: serverCalls.slice().sort((a, b) => Number(b[7] || 0) - Number(a[7] || 0)).slice(0, 20).map(row => ({
      operation: String(row[9] || ''), duration_ms: Number(row[7]) || 0, status: String(row[8] || '')})),
    events: selected.slice(-1000).reverse().map(row => ({
      timestamp: row[0] instanceof Date ? row[0].toISOString() : String(row[0] || ''),
      user: String(row[1] || ''), session_id: String(row[2] || ''), name: String(row[3] || ''),
      area: String(row[4] || ''), section: String(row[5] || ''), view: String(row[6] || ''),
      duration_ms: Number(row[7]) || 0, status: String(row[8] || ''), detail: String(row[9] || ''),
      version: row[10] instanceof Date ? 'unknown' : String(row[10] || '')}))
  };
  const serialized = JSON.stringify(report);
  if (serialized.length < 90000) cache.put(cacheKey, serialized, 60);
  return report;
}
