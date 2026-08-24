function getNewsletterSettings() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  return {
    recipients: _property_('NPS_LENS_NEWSLETTER_RECIPIENTS'),
    subject: _property_('NPS_LENS_NEWSLETTER_SUBJECT') || 'NPS Lens · Voz del cliente y causalidad operativa'
  };
}

function saveNewsletterSettings(payload) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const recipients = String(payload && payload.recipients || '').split(',').map(value => value.trim().toLowerCase()).filter(Boolean);
  if (recipients.some(email => !email.endsWith('@' + NPS_LENS.domain))) throw new Error('Todos los destinatarios deben pertenecer al dominio BBVA.');
  PropertiesService.getScriptProperties().setProperties({
    NPS_LENS_NEWSLETTER_RECIPIENTS: recipients.join(','),
    NPS_LENS_NEWSLETTER_SUBJECT: String(payload && payload.subject || '').trim()
  });
  return getNewsletterSettings();
}

function sendNewsletter() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const settings = getNewsletterSettings();
  if (!settings.recipients) throw new Error('Configura al menos un destinatario.');
  const edition = _publishedEdition_();
  const dashboard = edition.screens && edition.screens.dashboard || {};
  const kpis = dashboard.kpis || {};
  const webUrl = ScriptApp.getService().getUrl();
  const reportUrl = getReportUrl();
  const htmlBody = '<div style="font-family:Arial;color:#070e46;max-width:680px">' +
    '<div style="background:#070e46;color:#fff;padding:28px"><h1>NPS Lens</h1><p>BBVA Banca de Empresas e Instituciones</p></div>' +
    '<div style="padding:28px"><h2>La señal del cliente, conectada con la operación</h2><p>' + String(dashboard.context_label || 'Edición actualizada') + '</p>' +
    '<p><b>Muestras:</b> ' + String(kpis.samples || 'n/d') + ' · <b>NPS clásico:</b> ' + String(kpis.classic_nps || 'n/d') + '</p>' +
    '<p><a href="' + webUrl + '" style="background:#001391;color:#fff;padding:12px 16px;text-decoration:none">Abrir NPS Lens</a> ' +
    (reportUrl ? '<a href="' + reportUrl + '" style="color:#001391;padding:12px">Abrir presentación</a>' : '') + '</p></div></div>';
  MailApp.sendEmail({to: settings.recipients, subject: settings.subject, htmlBody, name: 'NPS Lens'});
  return {sent: settings.recipients.split(',').length, at: new Date().toISOString()};
}
