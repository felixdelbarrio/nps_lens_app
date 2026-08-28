const NEWSLETTER_RECIPIENT_HEADERS = Object.freeze([
  'email', 'active', 'created_at', 'created_by', 'updated_at', 'updated_by'
]);

function _newsletterRecipients_() {
  const sheet = _sheet_(NPS_LENS.recipientsSheet);
  if (sheet.getLastRow() <= 1) return [];
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, NEWSLETTER_RECIPIENT_HEADERS.length).getValues()
    .map((row, index) => ({row: index + 2, email: String(row[0] || '').toLowerCase(), active: row[1] === true}));
}

function getNewsletterSettings() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const recipients = _newsletterRecipients_();
  return {
    subject: _property_('NPS_LENS_NEWSLETTER_SUBJECT') || 'NPS Lens · Voz del cliente y causalidad operativa',
    recipients: recipients.map(item => ({email: item.email, active: item.active})),
    activeCount: recipients.filter(item => item.active).length,
    presentationUrl: _reportUrl_()
  };
}

function saveNewsletterSubject(subject) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const clean = _cleanText_(subject, 180);
  if (!clean) throw new Error('Indica un asunto para la newsletter.');
  PropertiesService.getScriptProperties().setProperty('NPS_LENS_NEWSLETTER_SUBJECT', clean);
  return {subject: clean};
}

function saveNewsletterRecipient(payload) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const email = _cleanText_(payload && payload.email, 180).toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || !email.endsWith('@' + NPS_LENS.domain)) {
    throw new Error('Indica un correo válido del dominio BBVA.');
  }
  const active = !payload || payload.active !== false;
  const sheet = _sheet_(NPS_LENS.recipientsSheet);
  const existing = _newsletterRecipients_().find(item => item.email === email);
  const now = new Date();
  if (existing) {
    const created = sheet.getRange(existing.row, 3, 1, 2).getValues()[0];
    sheet.getRange(existing.row, 2, 1, 5).setValues([[active, created[0], created[1], now, viewer.email]]);
  } else {
    sheet.appendRow([email, active, now, viewer.email, now, viewer.email]);
  }
  return {email, active};
}

function _newsletterInsight_(edition) {
  const dashboard = edition.screens && edition.screens.dashboard || {};
  const kpis = dashboard.kpis || {};
  const opportunities = dashboard.opportunities && dashboard.opportunities.table || [];
  const opportunity = opportunities.length ? String(opportunities[0].opportunity || opportunities[0].topic || opportunities[0].name || '') : '';
  return {context: _cleanText_(dashboard.context_label || 'Edición actualizada', 240),
    samples: kpis.samples == null ? 'n/d' : String(kpis.samples),
    nps: kpis.classic_nps == null ? 'n/d' : String(kpis.classic_nps), opportunity: _cleanText_(opportunity, 240)};
}

function _newsletterEscape_(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, character => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[character]);
}

function _publishedNewsletterInsight_() {
  const stored = _property_(NPS_LENS.newsletterInsightProperty);
  if (!stored) throw new Error('La edición actual no contiene el insight de newsletter. Vuelve a importarla.');
  try { return JSON.parse(stored); }
  catch (error) { throw new Error('El insight de newsletter publicado no es válido. Vuelve a importar la edición.'); }
}

function _newsletterHtml_(insight, reportUrl) {
  const webUrl = ScriptApp.getService().getUrl();
  const opportunity = insight.opportunity ? '<div style="margin:20px 0;padding:18px;background:#EAF3FA;border-left:4px solid #2DCCCD"><b>Foco ejecutivo</b><br>' + _newsletterEscape_(insight.opportunity) + '</div>' : '';
  return '<div style="font-family:Arial,sans-serif;color:#121F3F;max-width:680px;margin:auto;background:#F4F6F8">' +
    '<div style="background:#070E46;color:#fff;padding:32px"><div style="font-size:12px;letter-spacing:1.2px">BBVA BANCA DE EMPRESAS E INSTITUCIONES</div><h1 style="margin:12px 0 4px">NPS Lens</h1><div>La voz del cliente conectada con la operación</div></div>' +
    '<div style="padding:32px;background:#fff"><h2 style="font-family:Georgia,serif;color:#070E46">Una lectura preparada para decidir</h2><p>' + _newsletterEscape_(insight.context) + '</p>' +
    '<div style="display:flex;gap:12px"><div style="padding:14px;background:#F4F6F8;min-width:120px"><small>Muestras</small><br><b style="font-size:24px">' + _newsletterEscape_(insight.samples) + '</b></div><div style="padding:14px;background:#F4F6F8;min-width:120px"><small>NPS clásico</small><br><b style="font-size:24px">' + _newsletterEscape_(insight.nps) + '</b></div></div>' + opportunity +
    '<p><a href="' + webUrl + '" style="display:inline-block;background:#001391;color:#fff;padding:13px 18px;text-decoration:none;font-weight:bold">Abrir NPS Lens</a>' +
    (reportUrl ? ' <a href="' + reportUrl + '" style="display:inline-block;color:#001391;padding:13px 18px;font-weight:bold">Abrir presentación en Google Slides</a>' : '') + '</p></div></div>';
}

function _sendNewsletterTo_(recipients, subject) {
  const reportUrl = _reportUrl_();
  if (!reportUrl) throw new Error('Publica primero una edición con su presentación nativa.');
  MailApp.sendEmail({to: recipients.join(','), subject, htmlBody: _newsletterHtml_(_publishedNewsletterInsight_(), reportUrl), name: 'NPS Lens'});
  return {sent: recipients.length, at: new Date().toISOString(), presentationUrl: reportUrl};
}

function testNewsletter() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const subject = _property_('NPS_LENS_NEWSLETTER_SUBJECT') || 'NPS Lens · Voz del cliente y causalidad operativa';
  return _sendNewsletterTo_([viewer.email], '[PRUEBA] ' + subject);
}

function sendNewsletter() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const settings = getNewsletterSettings();
  const active = settings.recipients.filter(item => item.active).map(item => item.email);
  if (!active.length) throw new Error('Activa al menos un destinatario.');
  return _sendNewsletterTo_(active, settings.subject);
}
